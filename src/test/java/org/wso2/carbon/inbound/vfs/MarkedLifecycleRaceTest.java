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

import org.apache.synapse.core.SynapseEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.carbon.inbound.vfs.poll.Lifecycle;
import org.wso2.carbon.inbound.vfs.poll.LifecycleTest;
import org.wso2.carbon.inbound.vfs.poll.LifecycleTest.Call;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.health.HealthRegistry;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("testbuild")
class MarkedLifecycleRaceTest {
    @TempDir Path root;

    @Test
    void everyCleanupOrderHoldsForTwoHundredRoundsAtEachRealBarrier() throws Exception {
        String previous = System.setProperty("vfs.testpoints.dir", root.toString());
        String name = "lifecycle" + UUID.randomUUID();
        List<String> results = new ArrayList<>();
        List<String> observations = new ArrayList<>();
        Map<Integer, String> failures = new LinkedHashMap<>();
        try {
            TestPoints.copy("inbound", VFSConsumer.class);
            TestPoints.arguments("stop.after-retire", name);
            Path folder;
            try (java.util.stream.Stream<Path> files = Files.list(root)) {
                folder = files.filter(Files::isDirectory).findFirst().get();
            }
            for (String point : Arrays.asList("stop.after-retire", "stop.before-final-step")) {
                int reached = 0;
                for (String order : Arrays.asList("resume", "destroy", "destroy resume", "resume destroy")) {
                    for (int round = 0; round < 200; round++) {
                        observations.add(runCase(name, folder, point, ++reached, order, round + 1, failures));
                    }
                    results.add(point + "\t" + order + "\t200");
                }
            }
            Path report = java.nio.file.Paths.get("target", "surefire-reports", "lifecycle-race-rounds.tsv");
            Files.createDirectories(report.getParent());
            Files.write(report, results, StandardCharsets.UTF_8);
            Files.write(report.resolveSibling("lifecycle-race-observations.tsv"), observations, StandardCharsets.UTF_8);
            assertTrue(failures.isEmpty(), () -> String.join(", ", failures.values()));
        } finally {
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, name);
            if (previous == null) {
                System.clearProperty("vfs.testpoints.dir");
            } else {
                System.setProperty("vfs.testpoints.dir", previous);
            }
        }
    }

    // The checks of a round, by number.
    private static final String[] CHECKS = {"", "resume waits for the drain", "new generation starts after done",
            "health paused and then ok", "second call waits for the drain", "final state follows the order",
            "old runner leaves the new sessions alone"};

    private void check(Map<Integer, String> failures, int row, boolean observed, String detail) {
        if (!observed) {
            failures.putIfAbsent(row, CHECKS[row] + ": " + detail);
        }
    }

    private String runCase(String name, Path folder, String point, int reached, String order, int round,
                           Map<Integer, String> failures) throws Exception {
        Properties p = new Properties();
        p.setProperty("transport.vfs.FileURI", root.toUri().toString());
        p.setProperty("transport.vfs.StopTimeout", "0ms");
        List<VfsSession> sessions = Collections.synchronizedList(new ArrayList<>());
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        EndpointHealth health = HealthRegistry.register(HealthRegistry.Kind.INBOUND, name, 1000);
        AtomicReference<Lifecycle> lifecycle = new AtomicReference<>();
        List<String> atBuild = new ArrayList<>();
        VFSConsumer consumer = new VFSConsumer(p, name, mock(SynapseEnvironment.class), 1000,
                "main", null, false, true) {
            @Override
            VfsSession newSession(long number) {
                if (number > 1) {
                    String state = LifecycleTest.snapshot(lifecycle.get());
                    assertTrue(state.contains("done=true"), "new generation starts after done " + state);
                    atBuild.add(health.state().name());
                    assertEquals(EndpointHealth.State.PAUSED, health.state(), "health paused before the new generation");
                }
                VfsSession session = mock(VfsSession.class);
                doAnswer(call -> events.add("abort " + number + " " + Thread.currentThread().getName())).when(session).abort();
                doAnswer(call -> events.add("close " + number + " " + Thread.currentThread().getName())).when(session).closeAfterAbort();
                sessions.add(session);
                return session;
            }
        };
        Lifecycle life = VFSConsumerLifecycleTest.field(consumer, "lifecycle", Lifecycle.class);
        lifecycle.set(life);
        Generation old = life.enterPoll();
        assertTrue(life.fileStarted(old));
        old.pollStarted();
        Path hold = folder.resolve(point + ".hold");
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(name.getBytes(StandardCharsets.UTF_8));
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            key.append(String.format("%02x", hash[i]));
        }
        Path witness = folder.resolve(point + "@k" + key + "." + reached + ".reached");
        Files.write(hold, Arrays.asList("generation=1", "endpoint=" + name, "wait=5"), StandardCharsets.UTF_8);
        Call runner = new Call("drain-one", consumer::destroy);
        Call first;
        Call second = null;
        String held;
        try {
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.isRegularFile(witness) && System.nanoTime() < limit) {
                Thread.sleep(1);
            }
            assertTrue(Files.isRegularFile(witness), witness.toString());
            assertTrue(old.isRetired());
            assertEquals(Lifecycle.State.DRAINING, life.state());
            assertEquals(EndpointHealth.State.OK, health.state());
            boolean resumeFirst = order.startsWith("resume");
            first = new Call("wait-one", resumeFirst ? consumer::resume : consumer::destroy);
            first.waitingAt("await");
            if (order.contains(" ")) {
                second = new Call("wait-two", resumeFirst ? consumer::destroy : consumer::resume);
                second.waitingAt("await");
            }
            held = LifecycleTest.snapshot(life);
            assertTrue(held.contains("state=DRAINING") && held.contains("done=false"), "second call waits for the drain " + held);
            assertFalse(first.hasReturned(), resumeFirst ? "resume waits for the drain" : "second call waits for the drain");
            if (second != null) {
                assertFalse(second.hasReturned(), resumeFirst ? "second call waits for the drain" : "resume waits for the drain");
            }
            assertEquals(1, sessions.size(), "resume waits for the drain");
        } finally {
            Files.deleteIfExists(hold);
        }
        runner.join();
        first.join();
        if (second != null) {
            second.join();
        }
        check(failures, 2, events.contains("abort 1 drain-one")
                && events.stream().noneMatch(s -> s.startsWith("close 1 ")), "old session before exit");
        life.fileEnded(old, null);
        life.exitPoll(old);
        check(failures, 2, events.contains("close 1 " + Thread.currentThread().getName()), "old owner close");
        boolean stopped = order.endsWith("destroy");
        Lifecycle.State state = life.state();
        check(failures, order.contains(" ") ? 5 : (stopped ? 4 : 1),
                state == (stopped ? Lifecycle.State.STOPPED : Lifecycle.State.RUNNING), order + " returned " + state);
        check(failures, 3, health.state() == (state == Lifecycle.State.STOPPED
                ? EndpointHealth.State.PAUSED : EndpointHealth.State.OK), "health after " + order);
        check(failures, 1, sessions.size() == (order.contains("resume") ? 2 : 1), "manager count");
        if ("resume destroy".equals(order)) {
            check(failures, 5, events.contains("abort 2 wait-two") && events.contains("close 2 wait-two"),
                    "new session was not drained by the waiting destroy");
        } else {
            check(failures, 5, events.stream().noneMatch(s -> s.startsWith("abort 2 ") || s.startsWith("close 2 ")),
                    "unexpected drain of new session");
        }
        check(failures, 6, events.stream().noneMatch(s -> s.endsWith("2 drain-one")), "old runner on new session");
        return String.join("\t", point, order, Integer.toString(round), life.state().name(), health.state().name(),
                String.join(",", atBuild), Integer.toString(sessions.size()), String.join("|", events),
                held.replace('\t', ','), LifecycleTest.snapshot(life).replace('\t', ','));
    }
}
