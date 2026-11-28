/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.carbon.inbound.vfs.poll;

import org.junit.jupiter.api.Test;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.health.HealthRegistry;
import org.wso2.vfs.shared.lock.HeldLock;
import org.wso2.vfs.shared.lock.ShareLock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LateGenerationTest {

    private static class FileWork {
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Thread> closer = new AtomicReference<>();
        final AtomicInteger normalActions = new AtomicInteger();
        final HeldLock handle;
        final LifecycleTest.Call call;

        FileWork(String name, Lifecycle life, Generation generation, VfsSession session, ShareLock locks) {
            assertTrue(life.fileStarted(generation));
            handle = spy(locks.enterLocalOnly("file:///tmp/" + name, 1, 0).handle);
            doAnswer(inv -> { closer.set(Thread.currentThread()); return null; }).when(session).closeAfterAbort();
            call = new LifecycleTest.Call(name, () -> {
                LifecycleTest.await(start);
                LifecycleTest.await(release);
                try {
                    if (generation.isRetired()) {
                        handle.abandon();
                        generation.fileDone();
                        generation.openSessions(77);
                        generation.progress(64);
                        generation.flag(EndpointHealth.Flag.READ_CAP, name);
                    } else {
                        normalActions.incrementAndGet();
                        generation.fileDone();
                        handle.abandon();
                    }
                } finally {
                    life.fileEnded(generation, session);
                }
            });
            assertTrue(life.handOver(generation, session, call.thread));
            start.countDown();
        }

        void end() throws Exception {
            release.countDown();
            call.join();
        }
    }

    @Test
    void lateFileFinishesOnItsOwnGenerationAndItsOwnerCloses() throws Exception {
        run(false);
    }

    @Test
    void stoppingTheNewGenerationDoesNotWaitForAnotherLateWorker() throws Exception {
        run(true);
    }

    private void run(boolean keepAnotherWorker) throws Exception {
        String name = UUID.randomUUID().toString();
        EndpointHealth health = LifecycleTest.health(name);
        ShareLock locks = ShareLock.create(name, true, 60000, health);
        List<VfsSession> old = Arrays.asList(mock(VfsSession.class), mock(VfsSession.class), mock(VfsSession.class));
        List<VfsSession> fresh = Arrays.asList(mock(VfsSession.class), mock(VfsSession.class), mock(VfsSession.class));
        Lifecycle life = new Lifecycle(name, health, number -> number == 1 ? old : fresh, () -> 1000);
        try {
            Generation first = life.enterPoll();
            FileWork a = new FileWork(name + "A", life, first, old.get(1), locks);
            FileWork c = keepAnotherWorker ? new FileWork(name + "C", life, first, old.get(2), locks) : null;
            life.exitPoll(first);
            long start = System.nanoTime();
            life.stop("destroy");
            long elapsed = (System.nanoTime() - start) / 1000000;
            assertTrue(elapsed >= 995 && elapsed < 2000, "elapsed=" + elapsed);
            assertTrue(first.isRetired());
            assertEquals(keepAnotherWorker ? 2 : 1, first.inFlight, "old generation counts");
            verify(old.get(1), never().description("late file ends alone")).closeAfterAbort();
            life.resume();
            Generation next = life.enterPoll();
            next.pollStarted();
            FileWork b = new FileWork(name + "B", life, next, fresh.get(1), locks);
            Map<String, Object> before = health.snapshot();
            int oldBefore = first.inFlight;
            int newBefore = next.inFlight;
            boolean pollBefore = next.pollActive;
            a.end();
            assertEquals(keepAnotherWorker ? 1 : 0, first.inFlight, "old generation counts");
            assertEquals(1, next.inFlight, "new generation unchanged");
            assertTrue(next.pollActive, "new generation unchanged");
            Map<String, Object> after = health.snapshot();
            for (String key : Arrays.asList("state", "filesDone", "filesFailed", "flags", "openSessions")) {
                assertEquals(before.get(key), after.get(key), "new generation unchanged " + key);
            }
            assertEquals(0, a.normalActions.get(), "late file ends alone");
            verify(a.handle, times(1).description("late file ends alone")).abandon();
            verify(a.handle, never().description("late file ends alone")).pendingStateWrite();
            verify(a.handle, never().description("late file ends alone")).unlockShare(any());
            verify(a.handle, never().description("late file ends alone")).releaseChecked(any());
            verify(a.handle, never().description("late file ends alone")).releaseAfterAction(any(), any());
            verify(old.get(1), times(1).description("late file ends alone")).closeAfterAbort();
            assertSame(a.call.thread, a.closer.get(), "late file ends alone");
            String counts = oldBefore + "\t" + first.inFlight + "\t" + newBefore + "\t" + next.inFlight
                    + "\t" + pollBefore + "\t" + next.pollActive;
            LifecycleTest.Call stop = new LifecycleTest.Call("stop-new-generation", () -> life.stop("pause"));
            stop.waitingAt("drain");
            b.end();
            life.exitPoll(next);
            stop.join();
            assertEquals(1, b.normalActions.get(), "new drain does not wait for the old worker");
            boolean anotherHeld = c != null && !c.call.task.isDone();
            if (c != null) {
                assertFalse(c.call.task.isDone(), "new drain does not wait for the old worker");
                assertEquals(1, first.inFlight, "new drain does not wait for the old worker");
                verify(old.get(2), never().description("new drain does not wait for the old worker")).closeAfterAbort();
                c.end();
                assertSame(c.call.thread, c.closer.get(), "new drain does not wait for the old worker");
                assertEquals(0, first.inFlight, "new drain does not wait for the old worker");
            }
            Path report = Paths.get("target", "surefire-reports", keepAnotherWorker ? "late-two.tsv" : "late-one.tsv");
            Files.createDirectories(report.getParent());
            Files.write(report, Arrays.asList("elapsedMs\toldBefore\toldAfter\tnewBefore\tnewAfter\tpollBefore"
                            + "\tpollAfter\tstateBefore\tstateAfter\tlateBefore\tlateAfter\tnormalActions\townerClosed"
                            + "\totherStillHeld\tnewState",
                    elapsed + "\t" + counts + "\t" + before.get("state") + "\t" + after.get("state")
                            + "\t" + before.get("lateThreads") + "\t" + after.get("lateThreads")
                            + "\t" + a.normalActions.get() + "\t" + (a.closer.get() == a.call.thread)
                            + "\t" + anotherHeld + "\t" + life.state()), StandardCharsets.UTF_8);
            assertEquals(((Number) before.get("lateThreads")).longValue() + 1, after.get("lateThreads"), "old generation counts");
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }
}
