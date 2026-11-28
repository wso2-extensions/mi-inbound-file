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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.carbon.inbound.vfs.poll.Lifecycle;
import org.wso2.carbon.inbound.vfs.poll.ReadErrorCounter;
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.health.HealthRegistry;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.wso2.carbon.inbound.vfs.VFSConsumerLifecycleTest.field;
import static org.wso2.vfs.shared.failstate.FileIdentity.TimeSource.COMMAND;
import static org.wso2.vfs.shared.failstate.FileIdentity.TimeSource.LISTING;

class ReadCapOwnershipTest {
    @TempDir Path root;

    private static class Active {
        final VFSConsumer consumer;
        final ReadErrorCounter counter;
        final Lifecycle life;
        final EndpointHealth health;
        final Generation generation;

        Active(VFSConsumer consumer) throws Exception {
            this.consumer = consumer;
            counter = field(consumer, "readErrors", ReadErrorCounter.class);
            life = field(consumer, "lifecycle", Lifecycle.class);
            health = field(consumer, "health", EndpointHealth.class);
            generation = life.enterPoll();
        }

        void flag(FileIdentity file) {
            counter.transportFailure(generation, file, true);
            counter.flagCapped(generation, file);
        }

        long count(String kind) {
            Map<?, ?> flags = (Map<?, ?>) health.snapshot().get("flags");
            Map<?, ?> detail = (Map<?, ?>) flags.get(kind);
            return detail == null ? 0L : (Long) detail.get("count");
        }

        void end() {
            life.exitPoll(generation);
            consumer.destroy();
        }
    }

    private Active start(String name, int cap, boolean cron) throws Exception {
        Properties options = new Properties();
        options.setProperty("transport.vfs.FileURI", root.toUri().toString());
        options.setProperty("transport.vfs.MaxReadTransportErrors", Integer.toString(cap));
        options.setProperty("transport.vfs.StopTimeout", "1ms");
        return new Active(cron ? new VFSConsumer(options, name, null, "0/10 * * * * ?", "main", null, false, true)
                : new VFSConsumer(options, name, null, 1000, "main", null, false, true));
    }

    private static FileIdentity file(int n) {
        return new FileIdentity("file:///in/" + n, 1, 2, COMMAND, null);
    }

    @Test
    void replacementResolvesSampledAndUnsampledOwnedFlagsAndUsesItsOwnThreshold() throws Exception {
        String name = UUID.randomUUID().toString();
        String other = UUID.randomUUID().toString();
        try {
            Active first = start(name, 1, false);
            first.health.flag(EndpointHealth.Flag.STATE_WRITE, file(0).uri);
            first.health.flag(EndpointHealth.Flag.READ_CAP, file(999).uri);
            for (int i = 0; i < 105; i++) {
                first.flag(file(i));
            }
            assertEquals(106, first.count("READ_CAP"));
            Map<?, ?> flags = (Map<?, ?>) first.health.snapshot().get("flags");
            assertEquals(100, ((List<?>) ((Map<?, ?>) flags.get("READ_CAP")).get("paths")).size());
            first.consumer.destroy();
            Active replacement = start(name, 2, true);
            Active independent = start(other, 1, false);
            assertSame(first.health, replacement.health);
            independent.flag(file(104));
            replacement.counter.goodRead(replacement.generation, file(104));
            assertEquals(105, replacement.count("READ_CAP"));
            replacement.counter.listed(replacement.generation, Collections.singleton(file(0).uri));
            assertEquals(2, replacement.count("READ_CAP"));
            first.counter.goodRead(first.generation, file(0));
            first.counter.listed(first.generation, Collections.emptySet());
            first.flag(file(105));
            first.life.exitPoll(first.generation);
            assertEquals(2, replacement.count("READ_CAP"));
            assertEquals(1, independent.count("READ_CAP"));
            assertFalse(replacement.counter.capped(replacement.generation, file(0)));
            replacement.flag(file(0));
            assertFalse(replacement.counter.capped(replacement.generation, file(0)));
            replacement.flag(file(0));
            assertTrue(replacement.counter.capped(replacement.generation, file(0)));
            assertEquals(2, replacement.count("READ_CAP"));
            replacement.counter.goodRead(replacement.generation, file(0));
            assertEquals(1, replacement.count("READ_CAP"));
            assertEquals(1, replacement.count("STATE_WRITE"));
            independent.end();
            replacement.end();
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, other);
        }
    }

    @Test
    void resumeRetainsTheCountAndReplacementRetainsBothObservedTimes() throws Exception {
        String name = UUID.randomUUID().toString();
        try {
            Active old = start(name, 1, false);
            FileIdentity command = file(7);
            FileIdentity listing = new FileIdentity(command.uri, 1, 400, LISTING, null);
            old.flag(command);
            assertTrue(old.counter.capped(old.generation, listing));
            old.life.exitPoll(old.generation);
            old.consumer.pause();
            old.consumer.resume();
            Active resumed = new Active(old.consumer);
            assertTrue(resumed.counter.capped(resumed.generation, command));
            assertTrue(resumed.counter.capped(resumed.generation, listing));
            resumed.end();
            Active replacement = start(name, 1, true);
            assertFalse(replacement.counter.capped(replacement.generation, listing));
            assertEquals(1, replacement.count("READ_CAP"));
            FileIdentity changed = new FileIdentity(command.uri, 1, 401, LISTING, null);
            assertFalse(replacement.counter.capped(replacement.generation, changed));
            assertEquals(0, replacement.count("READ_CAP"));
            replacement.flag(changed);
            assertEquals(1, replacement.count("READ_CAP"));
            replacement.end();
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void anOlderOwnerCannotClearOrIntroduceMembershipAfterRegistrationTransfers() throws Exception {
        String name = UUID.randomUUID().toString();
        try {
            Active old = start(name, 1, false);
            old.flag(file(1));
            Active replacement = start(name, 1, true);
            old.counter.goodRead(old.generation, file(1));
            old.flag(file(2));
            assertEquals(1, replacement.count("READ_CAP"));
            replacement.counter.goodRead(replacement.generation, file(1));
            assertEquals(0, replacement.count("READ_CAP"));
            replacement.flag(file(2));
            old.counter.listed(old.generation, Collections.emptySet());
            assertEquals(1, replacement.count("READ_CAP"));
            replacement.counter.goodRead(replacement.generation, file(2));
            assertEquals(0, replacement.count("READ_CAP"));
            old.end();
            replacement.end();
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void eachTimeSourceComparesWithItsOwnLastObservationAndSizeAlwaysWins() throws Exception {
        for (FileIdentity.TimeSource first : FileIdentity.TimeSource.values()) {
            FileIdentity.TimeSource second = first == COMMAND ? LISTING : COMMAND;
            for (FileIdentity.TimeSource change : FileIdentity.TimeSource.values()) {
                String name = UUID.randomUUID().toString();
                try {
                    Active active = start(name, 2, false);
                    FileIdentity a = new FileIdentity(file(1).uri, 1, 20, first, null);
                    FileIdentity b = new FileIdentity(a.uri, 1, 400, second, null);
                    active.flag(a);
                    active.flag(b);
                    assertTrue(active.counter.capped(active.generation, a));
                    assertTrue(active.counter.capped(active.generation, b));
                    FileIdentity changed = new FileIdentity(a.uri, 1, 500, change, null);
                    assertFalse(active.counter.capped(active.generation, changed));
                    assertEquals(0, active.count("READ_CAP"));
                    active.flag(changed);
                    assertFalse(active.counter.capped(active.generation, changed));
                    active.flag(changed);
                    assertEquals(1, active.count("READ_CAP"));
                    FileIdentity resized = new FileIdentity(a.uri, 2, 500, second, null);
                    assertFalse(active.counter.capped(active.generation, resized));
                    assertEquals(0, active.count("READ_CAP"));
                    active.end();
                } finally {
                    HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
                }
            }
        }
    }
}
