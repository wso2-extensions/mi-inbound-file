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
package org.wso2.carbon.inbound.vfs;

import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.util.GracefulShutdownTimer;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.carbon.inbound.vfs.poll.Lifecycle;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.health.HealthRegistry;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VFSConsumerLifecycleTest {

    @TempDir Path root;

    static <T> T field(VFSConsumer consumer, String name, Class<T> type) throws Exception {
        Field field = VFSConsumer.class.getDeclaredField(name);
        field.setAccessible(true);
        return type.cast(field.get(consumer));
    }

    @Test
    void destroyedConsumerCanBeCollectedWhilePausedHealthRemainsRegistered() throws Exception {
        String name = UUID.randomUUID().toString();
        try {
            ReferenceQueue<VFSConsumer> collected = new ReferenceQueue<>();
            WeakReference<VFSConsumer> consumer = deployAndDestroy(name, collected);
            EndpointHealth health = HealthRegistry.register(HealthRegistry.Kind.INBOUND, name, 1000);
            for (int i = 0; i < 40; i++) {
                System.gc();
                if (collected.remove(50) == consumer) {
                    break;
                }
            }
            assertNull(consumer.get(), "Registered health retained the destroyed consumer");
            assertEquals(EndpointHealth.State.PAUSED, health.state());
            assertEquals(0L, health.snapshot().get("openSessions"));
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    private WeakReference<VFSConsumer> deployAndDestroy(String name, ReferenceQueue<VFSConsumer> collected) {
        Properties options = new Properties();
        options.setProperty("transport.vfs.FileURI", root.toUri().toString());
        VFSConsumer consumer = new VFSConsumer(options, name, null, 1000, "main", null, false, true);
        consumer.destroy();
        return new WeakReference<>(consumer, collected);
    }

    @Test
    void intervalAndCronBuildOnlyTheirConfiguredWorkerSessions() throws Exception {
        for (boolean cron : new boolean[]{false, true}) {
            for (boolean sequential : new boolean[]{true, false}) {
                Properties options = new Properties();
                options.setProperty("transport.vfs.FileURI", root.toUri().toString());
                options.setProperty("transport.vfs.WorkerPoolSize", "2");
                String name = UUID.randomUUID().toString();
                VFSConsumer consumer = cron
                        ? new VFSConsumer(options, name, null, "0/10 * * * * ?", "main", null, false, sequential)
                        : new VFSConsumer(options, name, null, 1000, "main", null, false, sequential);
                try {
                    Lifecycle life = field(consumer, "lifecycle", Lifecycle.class);
                    Generation first = life.enterPoll();
                    assertEquals(sequential, first.workerPool == null);
                    if (!sequential) {
                        first.workerPool.reserve().cancel();
                    }
                    life.exitPoll(first);
                    consumer.destroy();
                    consumer.resume();
                    Generation next = life.enterPoll();
                    assertEquals(2, next.number);
                    assertEquals(sequential, next.workerPool == null);
                    if (!sequential) {
                        assertNotSame(first.workerPool, next.workerPool);
                        next.workerPool.reserve().cancel();
                    }
                    life.exitPoll(next);
                } finally {
                    consumer.destroy();
                    HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
                }
            }
        }
    }

    @Test
    void resumeRetainsTheStoreAndEmitsNoFurtherOldListDiagnostic() throws Exception {
        Properties p = new Properties();
        p.setProperty("transport.vfs.FileURI", root.toUri().toString());
        p.setProperty("transport.vfs.FailedRecordsFileName", "list");
        p.setProperty("transport.vfs.FailedRecordsFileDestination", root.resolve("prefix").toString());
        p.setProperty(VFSConstants.TRANSPORT_FAILED_RECORD_TIMESTAMP_FORMAT, "yyyy");
        p.setProperty("transport.vfs.StopTimeout", "1");
        Path path = Files.write(root.resolve("prefixlist"), new byte[]{42});
        String name = UUID.randomUUID().toString();
        Logger logger = Logger.getLogger(EndpointHealth.class.getName());
        List<String> warnings = new ArrayList<>();
        Handler handler = new Handler() {
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING && record.getMessage().startsWith("[inbound:" + name + "]")) {
                    warnings.add(record.getMessage());
                }
            }
            public void flush() { }
            public void close() { }
        };
        logger.addHandler(handler);
        try {
            VFSConsumer consumer = new VFSConsumer(p, name, null, 10, "main", null, false, true);
            assertEquals(4, warnings.size(), warnings.toString());
            assertEquals(1, warnings.stream().filter(s -> s.contains("no longer uses")).count());
            for (String key : new String[]{"transport.vfs.FailedRecordsFileName",
                    "transport.vfs.FailedRecordsFileDestination", VFSConstants.TRANSPORT_FAILED_RECORD_TIMESTAMP_FORMAT}) {
                assertEquals(1, warnings.stream().filter(s -> s.contains(key + " is deprecated")).count());
            }
            Object store = field(consumer, "store", Object.class);
            EndpointHealth health = field(consumer, "health", EndpointHealth.class);
            Lifecycle life = field(consumer, "lifecycle", Lifecycle.class);
            assertEquals(EndpointHealth.State.PAUSED, health.state());
            // The stop ends its bound with the poll still out, so the store is kept for the resume.
            Generation first = life.enterPoll();
            consumer.destroy();
            life.exitPoll(first);
            consumer.resume();
            Generation next = life.enterPoll();
            assertNotSame(first.pollSession, next.pollSession);
            assertSame(store, field(consumer, "store", Object.class));
            assertSame(health, HealthRegistry.register(HealthRegistry.Kind.INBOUND, name, 10));
            assertArrayEquals(new byte[]{42}, Files.readAllBytes(path));
            assertEquals(4, warnings.size());
            life.exitPoll(next);
            consumer.destroy();
        } finally {
            logger.removeHandler(handler);
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void destroyReleasesTheStoreOnlyWhenNoWorkIsLeft() throws Exception {
        Properties p = new Properties();
        p.setProperty("transport.vfs.FileURI", root.toUri().toString());
        p.setProperty("transport.vfs.StopTimeout", "1");
        String name = UUID.randomUUID().toString();
        try {
            VFSConsumer consumer = new VFSConsumer(p, name, null, 10, "main", null, false, true);
            FailStateStore store = field(consumer, "store", FailStateStore.class);
            EndpointHealth health = field(consumer, "health", EndpointHealth.class);
            Lifecycle life = field(consumer, "lifecycle", Lifecycle.class);
            Generation out = life.enterPoll();
            // The stop bound ends with the poll still out, so the store stays for this name.
            consumer.destroy();
            assertSame(store, FailStateStore.forEndpoint(name, new FailStateStore.Parameters(), null, health));
            life.exitPoll(out);
            // A stop with no work left drops it, and the next deploy of the name gets a new store.
            consumer.resume();
            life.exitPoll(life.enterPoll());
            consumer.destroy();
            FailStateStore next = FailStateStore.forEndpoint(name, new FailStateStore.Parameters(), null, health);
            assertNotSame(store, next);
            FailStateStore.release(name, next);
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void aResumeAfterAReleaseUsesTheStoreThatIsRegisteredForTheName() throws Exception {
        Properties p = new Properties();
        p.setProperty("transport.vfs.FileURI", root.toUri().toString());
        String name = UUID.randomUUID().toString();
        try {
            VFSConsumer consumer = new VFSConsumer(p, name, null, 10, "main", null, false, true);
            EndpointHealth health = field(consumer, "health", EndpointHealth.class);
            Lifecycle life = field(consumer, "lifecycle", Lifecycle.class);
            life.exitPoll(life.enterPoll());
            consumer.destroy();
            consumer.resume();
            life.exitPoll(life.enterPoll());
            assertSame(FailStateStore.forEndpoint(name, new FailStateStore.Parameters(), null, health),
                    field(consumer, "store", FailStateStore.class));
            consumer.destroy();
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void usesTheServerDeadlineAndTheZeroGracefulShutdownBound() throws Exception {
        String name = UUID.randomUUID().toString();
        Axis2SynapseEnvironment env = mock(Axis2SynapseEnvironment.class);
        ConfigurationContext axis = mock(ConfigurationContext.class);
        when(env.getAxis2ConfigurationContext()).thenReturn(axis);
        Properties p = new Properties();
        p.setProperty("transport.vfs.FileURI", root.toUri().toString());
        p.setProperty("transport.vfs.StopTimeout", "60s");
        try (MockedStatic<GracefulShutdownTimer> timers = mockStatic(GracefulShutdownTimer.class)) {
            GracefulShutdownTimer timer = mock(GracefulShutdownTimer.class);
            timers.when(GracefulShutdownTimer::getInstance).thenReturn(timer);
            VFSConsumer consumer = new VFSConsumer(p, name, env, 10, "main", null, false, true);
            Lifecycle life = field(consumer, "lifecycle", Lifecycle.class);
            Generation first = life.enterPoll();
            when(timer.isStarted()).thenReturn(true);
            when(timer.getRemainingTimeMillis()).thenReturn(30L);
            long start = System.nanoTime();
            consumer.pause();
            long elapsed = (System.nanoTime() - start) / 1000000;
            assertTrue(elapsed >= 25 && elapsed < 1030, "elapsed=" + elapsed);
            assertTrue(first.isRetired());
            verify(timer).getRemainingTimeMillis();
            life.exitPoll(first);
            consumer.resume();
            Generation next = life.enterPoll();
            when(timer.isStarted()).thenReturn(false);
            when(axis.getProperty("local_current.server.status")).thenReturn("SHUTTING_DOWN");
            start = System.nanoTime();
            consumer.destroy();
            assertTrue(System.nanoTime() - start < 1000000000L);
            verify(axis).getProperty("local_current.server.status");
            assertTrue(next.isRetired());
            life.exitPoll(next);
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }
}
