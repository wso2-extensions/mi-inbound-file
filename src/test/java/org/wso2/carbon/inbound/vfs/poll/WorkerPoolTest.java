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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WorkerPoolTest {
    @Test
    void fatalWorkerIsReplacedWithoutRebuildingPollServices() throws Exception {
        String name = UUID.randomUUID().toString();
        EndpointHealth health = LifecycleTest.health(name);
        VfsSession poll = mock(VfsSession.class);
        VfsSession old = mock(VfsSession.class);
        VfsSession replacement = mock(VfsSession.class);
        AtomicInteger services = new AtomicInteger();
        AtomicInteger workers = new AtomicInteger();
        Lifecycle life = new Lifecycle(name, health, n -> {
            services.incrementAndGet();
            return Collections.singletonList(poll);
        }, () -> 1000, 1, n -> workers.getAndIncrement() == 0 ? old : replacement);
        Generation generation = life.enterPoll();
        try {
            FileTask failed = mock(FileTask.class);
            when(failed.process(old, null, false)).thenThrow(new AssertionError("Test worker failure."));
            assertTrue(life.fileStarted(generation));
            CompletableFuture<FileTask.Outcome> first = generation.workerPool.reserve().submit(failed);
            ExecutionException fault = assertThrows(ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS));
            assertTrue(fault.getCause() instanceof AssertionError);

            FileTask healthy = mock(FileTask.class);
            when(healthy.process(replacement, null, false)).thenReturn(FileTask.Outcome.PROCESSED);
            assertTrue(life.fileStarted(generation));
            CompletableFuture<FileTask.Outcome> second = generation.workerPool.reserve().submit(healthy);
            assertEquals(FileTask.Outcome.PROCESSED, second.get(5, TimeUnit.SECONDS));
            assertEquals(2, workers.get());
            assertEquals(1, services.get());
            verify(old).closeAfterAbort();
            verify(poll, never()).closeAfterAbort();
        } finally {
            life.exitPoll(generation);
            life.stop("destroy");
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
        verify(old).closeAfterAbort();
        verify(replacement).closeAfterAbort();
    }

    @Test
    void lateReplacementCannotEnterResumedGeneration() throws Exception {
        String name = UUID.randomUUID().toString();
        EndpointHealth health = LifecycleTest.health(name);
        CountDownLatch replacing = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        VfsSession old = mock(VfsSession.class);
        VfsSession late = mock(VfsSession.class);
        VfsSession fresh = mock(VfsSession.class);
        AtomicInteger attempts = new AtomicInteger();
        Lifecycle life = new Lifecycle(name, health, n -> Collections.singletonList(mock(VfsSession.class)),
                () -> 0L, 1, number -> {
                    if (number > 1) {
                        return fresh;
                    }
                    if (attempts.getAndIncrement() == 0) {
                        return old;
                    }
                    replacing.countDown();
                    LifecycleTest.await(proceed);
                    return late;
                });
        Generation generation = life.enterPoll();
        try {
            FileTask task = mock(FileTask.class);
            when(task.process(old, null, false)).thenThrow(new AssertionError("Test worker failure."));
            assertTrue(life.fileStarted(generation));
            CompletableFuture<FileTask.Outcome> result = generation.workerPool.reserve().submit(task);
            assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
            assertTrue(replacing.await(5, TimeUnit.SECONDS));
            life.exitPoll(generation);
            life.stop("pause");
            life.resume();
            generation = life.enterPoll();
            proceed.countDown();
            verify(late, timeout(5000)).closeAfterAbort();
            verify(fresh, never()).closeAfterAbort();
            assertEquals(2, generation.number);
            generation.workerPool.reserve().cancel();
        } finally {
            proceed.countDown();
            life.exitPoll(generation);
            life.stop("destroy");
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }
}

