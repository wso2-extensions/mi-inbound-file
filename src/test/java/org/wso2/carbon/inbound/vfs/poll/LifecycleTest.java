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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongFunction;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class LifecycleTest {

    public static String snapshot(Lifecycle life) {
        return life.snapshot();
    }

    public static final class Call {
        final Thread thread;
        final FutureTask<Void> task;

        public Call(String name, Runnable run) {
            task = new FutureTask<>(run, null);
            thread = new Thread(task, name);
            thread.setDaemon(true);
            thread.start();
        }

        public void waitingAt(String method) throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < end) {
                if (task.isDone()) {
                    join();
                    fail("Call finished early: " + method);
                }
                boolean waiting = thread.getState() == Thread.State.WAITING
                        || thread.getState() == Thread.State.TIMED_WAITING;
                if (waiting && Arrays.stream(thread.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().equals(Lifecycle.class.getName())
                                && frame.getMethodName().equals(method))) {
                    return;
                }
                Thread.sleep(1);
            }
            fail("Call did not wait: " + method);
        }

        public boolean hasReturned() {
            return task.isDone();
        }

        public void join() throws Exception {
            try {
                task.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException failure) {
                throw new AssertionError(failure.getCause());
            }
        }
    }

    static EndpointHealth health(String name) {
        return HealthRegistry.register(HealthRegistry.Kind.INBOUND, name, 1000);
    }

    static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Barrier expired.");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    @Test
    void joiningPauseReturnsAfterTheDestroyInfo() throws Exception {
        String name = UUID.randomUUID().toString();
        CountDownLatch logging = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Logger logger = Logger.getLogger(Lifecycle.class.getName());
        Handler handler = new Handler() {
            public void publish(LogRecord record) {
                if (record.getMessage().contains("[inbound:" + name + "] destroy completed")) {
                    logging.countDown();
                    await(release);
                }
            }
            public void flush() { }
            public void close() { }
        };
        logger.addHandler(handler);
        try {
            Lifecycle life = new Lifecycle(name, health(name), n -> Arrays.asList(mock(VfsSession.class)), () -> 60000);
            Generation generation = life.enterPoll();
            Call destroy = new Call("destroy", () -> life.stop("destroy"));
            destroy.waitingAt("drain");
            Call pause = new Call("pause", () -> life.stop("pause"));
            pause.waitingAt("await");
            life.exitPoll(generation);
            await(logging);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (pause.thread.getState() != Thread.State.BLOCKED && !pause.hasReturned()
                    && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertEquals(Thread.State.BLOCKED, pause.thread.getState());
            release.countDown();
            destroy.join();
            pause.join();
        } finally {
            release.countDown();
            logger.removeHandler(handler);
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void freshGenerationAdmitsOnePollAndStandbyStaysPaused() {
        String name = UUID.randomUUID().toString();
        EndpointHealth health = health(name);
        try {
            VfsSession old = mock(VfsSession.class);
            VfsSession fresh = mock(VfsSession.class);
            AtomicInteger builds = new AtomicInteger();
            Lifecycle life = new Lifecycle(name, health, number -> Arrays.asList(
                    builds.getAndIncrement() == 0 ? old : fresh), () -> 0);
            assertEquals(EndpointHealth.State.PAUSED, health.state());
            life.resume();
            Generation first = life.enterPoll();
            assertNull(life.enterPoll());
            first.pollStarted();
            assertEquals(EndpointHealth.State.OK, health.state());
            life.exitPoll(first);
            life.stop("destroy");
            life.stop("destroy");
            assertNull(life.enterPoll());
            assertEquals(EndpointHealth.State.PAUSED, health.state());
            verify(old).abort();
            verify(old).closeAfterAbort();
            life.resume();
            life.resume();
            Generation next = life.enterPoll();
            assertEquals(first.number + 1, next.number);
            assertSame(fresh, next.pollSession);
            assertEquals(2, builds.get());
            life.exitPoll(next);
            life.stop("pause");
            verify(fresh).closeAfterAbort();
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void redeployAndHandoverStartWithPrivateSessionsOnTheNewObject() {
        String name = UUID.randomUUID().toString();
        try {
            EndpointHealth health = health(name);
            VfsSession old = mock(VfsSession.class);
            org.mockito.Mockito.when(old.getOpenClientCount()).thenReturn(2);
            Lifecycle previous = new Lifecycle(name, health, n -> Arrays.asList(old), () -> 0);
            Generation late = previous.enterPoll();
            late.pollStarted();
            assertEquals(2L, health.snapshot().get("openSessions"));
            previous.stop("destroy");
            previous.stop("destroy");
            assertEquals(0L, health.snapshot().get("openSessions"));
            verify(old, times(1)).abort();
            verify(old, never()).closeAfterAbort();

            VfsSession fresh = mock(VfsSession.class);
            org.mockito.Mockito.when(fresh.getOpenClientCount()).thenReturn(3);
            Lifecycle next = new Lifecycle(name, health, n -> Arrays.asList(fresh), () -> 0);
            next.resume();
            assertEquals(EndpointHealth.State.PAUSED, health.state());
            Generation first = next.enterPoll();
            assertEquals(1, first.number);
            assertSame(fresh, first.pollSession);
            first.pollStarted();
            previous.exitPoll(late);
            verify(old).closeAfterAbort();
            assertEquals(3L, health.snapshot().get("openSessions"));
            previous.stop("destroy");
            assertEquals(EndpointHealth.State.OK, health.state());
            assertEquals(Lifecycle.State.RUNNING, next.state());
            verify(fresh, never()).abort();
            next.exitPoll(first);
            next.stop("pause");
            next.stop("destroy");
            verify(fresh, times(1)).closeAfterAbort();
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void replacementOwnsHealthAcrossOldStopAndResume() {
        String name = UUID.randomUUID().toString();
        try {
            EndpointHealth health = health(name);
            Lifecycle old = new Lifecycle(name, health, n -> Arrays.asList(mock(VfsSession.class)), () -> 0);
            Lifecycle replacement = new Lifecycle(name, health, n -> {
                VfsSession session = mock(VfsSession.class);
                org.mockito.Mockito.when(session.getOpenClientCount()).thenReturn((int) n + 2);
                return Arrays.asList(session);
            }, () -> 0);
            Generation current = replacement.enterPoll();
            current.pollStarted();
            replacement.exitPoll(current);
            old.stop("destroy");
            assertEquals(EndpointHealth.State.OK, health.state());
            assertEquals(3L, health.snapshot().get("openSessions"));
            replacement.stop("pause");
            old.resume();
            assertEquals(EndpointHealth.State.PAUSED, health.state());
            assertEquals(0L, health.snapshot().get("openSessions"));
            replacement.resume();
            old.stop("destroy");
            assertEquals(EndpointHealth.State.OK, health.state());
            assertEquals(4L, health.snapshot().get("openSessions"));
            replacement.stop("destroy");
            assertEquals(EndpointHealth.State.PAUSED, health.state());
            assertEquals(0L, health.snapshot().get("openSessions"));
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void standbyUndeployAndFailedActivateLeaveNoWorkAdmitted() {
        String name = UUID.randomUUID().toString();
        try {
            VfsSession standby = mock(VfsSession.class);
            VfsSession recovered = mock(VfsSession.class);
            AtomicInteger builds = new AtomicInteger();
            Lifecycle life = new Lifecycle(name, health(name), n -> {
                int attempt = builds.incrementAndGet();
                if (attempt == 2) {
                    throw new IllegalStateException("Initialization failed.");
                }
                return Arrays.asList(attempt == 1 ? standby : recovered);
            }, () -> 0);
            life.stop("destroy");
            verify(standby).closeAfterAbort();
            life.resume();
            assertEquals(Lifecycle.State.STOPPED, life.state());
            assertNull(life.enterPoll());
            life.stop("destroy");
            life.resume();
            Generation active = life.enterPoll();
            assertEquals(2, active.number);
            assertSame(recovered, active.pollSession);
            life.exitPoll(active);
            life.stop("destroy");
            verify(recovered).closeAfterAbort();
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void shutdownJoinsTheDrainAndAnInterruptEndsItsWait() throws Exception {
        String name = UUID.randomUUID().toString();
        try {
            VfsSession session = mock(VfsSession.class);
            Lifecycle life = new Lifecycle(name, health(name), number -> Arrays.asList(session), () -> 60000);
            Generation active = life.enterPoll();
            assertTrue(life.fileStarted(active));
            Call pause = new Call("pause", () -> life.stop("pause"));
            pause.waitingAt("drain");
            Call destroy = new Call("destroy", () -> life.stop("destroy"));
            destroy.waitingAt("await");
            assertNull(life.enterPoll());
            assertFalse(life.fileStarted(active));
            pause.thread.interrupt();
            pause.join();
            destroy.join();
            assertTrue(active.isRetired());
            verify(session, times(1)).abort();
            verify(session, never()).closeAfterAbort();
            life.fileEnded(active, null);
            life.exitPoll(active);
            verify(session).closeAfterAbort();
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void bothCleanupBarriersPreserveBothOrdersOfArrival() throws Exception {
        for (String point : Arrays.asList("stop.after-retire", "stop.before-final-step")) {
            for (String order : Arrays.asList("resume", "destroy", "destroy resume", "resume destroy")) {
                race(point, order);
            }
        }
    }

    static void race(String point, String order) throws Exception {
        String name = UUID.randomUUID().toString();
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        VfsSession old = mock(VfsSession.class);
        VfsSession fresh = mock(VfsSession.class);
        AtomicInteger builds = new AtomicInteger();
        EndpointHealth health = health(name);
        try {
            Lifecycle life = new Lifecycle(name, health, number -> Arrays.asList(
                    builds.getAndIncrement() == 0 ? old : fresh), () -> 0) {
                @Override
                void point(String location, Drain drain) {
                    if (point.equals(location) && drain.generation.number == 1) {
                        reached.countDown();
                        await(release);
                    }
                }
            };
            Generation gen = life.enterPoll();
            gen.pollStarted();
            assertTrue(life.fileStarted(gen));
            Call runner = new Call("first-destroy", () -> life.stop("destroy"));
            await(reached);
            assertEquals(Lifecycle.State.DRAINING, life.state());
            boolean resumeFirst = order.startsWith("resume");
            Runnable resume = life::resume;
            Runnable destroy = () -> life.stop("destroy");
            Call first = new Call("first-waiter", resumeFirst ? resume : destroy);
            first.waitingAt("await");
            Call second = null;
            if (order.contains(" ")) {
                second = new Call("second-waiter", resumeFirst ? destroy : resume);
                second.waitingAt("await");
            }
            assertEquals(1, builds.get());
            assertFalse(runner.task.isDone());
            verify(fresh, never()).abort();
            release.countDown();
            runner.join();
            first.join();
            if (second != null) {
                second.join();
            }
            verify(old).abort();
            verify(old, never()).closeAfterAbort();
            life.fileEnded(gen, null);
            life.exitPoll(gen);
            verify(old).closeAfterAbort();
            boolean stopped = order.endsWith("destroy");
            assertEquals(stopped ? Lifecycle.State.STOPPED : Lifecycle.State.RUNNING, life.state());
            assertEquals(stopped ? EndpointHealth.State.PAUSED : EndpointHealth.State.OK, health.state());
            verify(fresh, times("resume destroy".equals(order) ? 1 : 0)).closeAfterAbort();
            assertEquals(order.contains("resume") ? 2 : 1, builds.get());
        } finally {
            release.countDown();
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void failedCleanupAbortsEverySessionAndResumeClosesEachManagerOnce() {
        for (boolean error : new boolean[] {false, true}) {
            String name = UUID.randomUUID().toString();
            try {
                List<VfsSession> old = Arrays.asList(mock(VfsSession.class), mock(VfsSession.class), mock(VfsSession.class));
                VfsSession next = mock(VfsSession.class);
                AtomicInteger builds = new AtomicInteger();
                LongFunction<List<VfsSession>> sessions = number -> builds.getAndIncrement() == 0 ? old : Arrays.asList(next);
                Lifecycle life = new Lifecycle(name, health(name), sessions, () -> 0) {
                    @Override
                    void point(String point, Drain record) {
                        if ("stop.after-retire".equals(point) && record.generation.number == 1) {
                            if (error) {
                                throw new AssertionError("test failure");
                            }
                            throw new IllegalStateException("test failure");
                        }
                    }
                };
                life.stop("destroy");
                assertTrue(life.snapshot().contains("done=true"));
                assertEquals(Lifecycle.State.STOPPED, life.state());
                old.forEach(session -> verify(session).abort());
                old.forEach(session -> verify(session, never()).closeAfterAbort());
                life.resume();
                old.forEach(session -> verify(session).closeAfterAbort());
                Generation generation = life.enterPoll();
                assertSame(next, generation.pollSession);
                life.exitPoll(generation);
            } finally {
                HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
            }
        }
    }

    @Test
    void onlyTheCurrentGenerationCanCompleteASkippedTick() {
        EndpointHealth health = mock(EndpointHealth.class);
        Lifecycle lifecycle = new Lifecycle("skip-guard", health,
                n -> java.util.Collections.singletonList(mock(VfsSession.class)), () -> 0L);
        Generation old = lifecycle.enterPoll();
        old.pollSkipped();
        verify(health).pollSkipped();
        lifecycle.exitPoll(old);
        lifecycle.stop("pause");
        lifecycle.resume();
        Generation current = lifecycle.enterPoll();
        old.pollSkipped();
        verify(health).pollSkipped();
        current.pollSkipped();
        verify(health, times(2)).pollSkipped();
        lifecycle.exitPoll(current);
        lifecycle.stop("destroy");
    }
}
