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
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.health.HealthRegistry;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ReadErrorCounterTest {
    private static FileIdentity file(long size, long time) {
        return new FileIdentity("file:///data/a", size, time, FileIdentity.TimeSource.COMMAND, null);
    }

    @Test
    void countsOnlyListedTransportFailuresAndFlagsOnceWhenTheCapIsApplied() {
        String name = UUID.randomUUID().toString();
        EndpointHealth health = LifecycleTest.health(name);
        try {
            Lifecycle life = new Lifecycle(name, health, n -> Collections.singletonList(mock(VfsSession.class)), () -> 0);
            Generation g = life.enterPoll();
            g.pollStarted();
            ReadErrorCounter counter = new ReadErrorCounter(3);
            FileIdentity file = file(7, 8);
            for (int i = 0; i < 5; i++) {
                counter.transportFailure(g, file, false);
            }
            for (int i = 0; i < 3; i++) {
                assertFalse(counter.capped(g, file));
                counter.transportFailure(g, file, true);
            }
            assertTrue(counter.capped(g, file));
            counter.flagCapped(g, file);
            Map<?, ?> flagged = (Map<?, ?>) health.snapshot().get("flags");
            Map<?, ?> detail = (Map<?, ?>) flagged.get("READ_CAP");
            assertEquals(1L, detail.get("count"));
            assertEquals(Collections.singletonList(file.uri), detail.get("paths"));
            counter.flagCapped(g, file);
            assertEquals(flagged, health.snapshot().get("flags"));
            counter.goodRead(g, file);
            assertFalse(counter.capped(g, file));
            assertFalse(((Map<?, ?>) health.snapshot().get("flags")).containsKey("READ_CAP"));
            life.exitPoll(g);
            life.stop("destroy");
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }

    @Test
    void aChangedIdentityOrAbsentPathClearsTheCountAndLateWorkCannotChangeIt() {
        String name = UUID.randomUUID().toString();
        try {
            Lifecycle life = new Lifecycle(name, LifecycleTest.health(name),
                    n -> Collections.singletonList(mock(VfsSession.class)), () -> 0);
            Generation g = life.enterPoll();
            ReadErrorCounter counter = new ReadErrorCounter(1);
            FileIdentity file = file(7, 8);
            counter.transportFailure(g, file, true);
            assertFalse(counter.capped(g, file(7, 9)));
            counter.transportFailure(g, file, true);
            assertFalse(counter.capped(g, file(8, 8)));
            counter.transportFailure(g, file, true);
            counter.listed(g, Collections.singleton(file.uri));
            assertTrue(counter.capped(g, file));
            life.stop("destroy");
            life.resume();
            Generation fresh = life.enterPoll();
            counter.goodRead(g, file);
            counter.listed(g, Collections.emptySet());
            assertTrue(counter.capped(fresh, file));
            counter.listed(fresh, Collections.emptySet());
            counter.transportFailure(g, file, true);
            assertFalse(counter.capped(fresh, file));
            life.exitPoll(g);
            life.exitPoll(fresh);
            life.stop("destroy");
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
        }
    }
}
