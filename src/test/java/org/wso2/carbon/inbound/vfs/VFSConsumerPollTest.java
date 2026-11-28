/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.inbound.vfs;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.common.SftpConstants;
import org.apache.sshd.sftp.server.SftpEventListener;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.org.apache.commons.vfs2.FileContent;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.provider.Fault;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.mockito.MockedStatic;
import org.wso2.vfs.shared.health.HealthRegistry;
import org.wso2.vfs.shared.failstate.FailState;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.lock.LockIo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.nio.charset.StandardCharsets;
import javax.xml.namespace.QName;
import org.apache.axis2.transport.http.ApplicationXMLFormatter;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.inbound.InboundEndpointConstants;
import org.apache.synapse.inbound.InboundResponseSender;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VFSConsumerPollTest {
    @TempDir Path folder;
    final String endpoint = "poll" + UUID.randomUUID();
    final Properties properties = new Properties();
    final SynapseConfiguration configuration = mock(SynapseConfiguration.class);
    final SequenceMediator sequence = mock(SequenceMediator.class);
    final List<String> deliveries = Collections.synchronizedList(new ArrayList<>());
    final List<String> threads = Collections.synchronizedList(new ArrayList<>());
    final Map<Thread, VfsSession> owners = new java.util.concurrent.ConcurrentHashMap<>();
    interface SessionSetup {
        void apply(VfsSession session) throws FileSystemException;
    }
    SessionSetup setupSession = session -> { };
    boolean resetDuringDelivery;
    long interval = 1000;
    VFSConsumer consumer;

    java.util.function.Consumer<Axis2MessageContext> mediation = message -> { };
    Builder xmlBuilder = new ApplicationXMLBuilder();

    SynapseEnvironment deploy(String uri, boolean sequential) throws Exception {
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(configuration.getSequence("process")).thenReturn(sequence);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn("poll");
        when(configuration.getInboundEndpoint(endpoint)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", xmlBuilder);
        axis.addMessageFormatter("application/xml", new ApplicationXMLFormatter());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            Axis2MessageContext message = call.getArgument(0);
            Map<?, ?> headers = (Map<?, ?>) message.getAxis2MessageContext().getProperty(MessageContext.TRANSPORT_HEADERS);
            deliveries.add(headers.get("RELATIVE_PATH") + "/" + headers.get(VFSConstants.FILE_NAME));
            threads.add(Thread.currentThread().getName());
            if (resetDuringDelivery) {
                VfsSession owner = owners.get(Thread.currentThread());
                try (FileObject scratch = owner.resolve("ram:///ordinary-count", owner.options(VFSConstants.TRANSPORT_FILE_FILE_URI))) {
                    assertTrue(owner.reset(scratch.getFileSystem(), null));
                }
            }
            mediation.accept(message);
            return true;
        });
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, uri);
        properties.putIfAbsent("transport.vfs.ActionAfterProcess", "DELETE");
        properties.putIfAbsent("transport.vfs.ActionAfterFailure", "NONE");
        properties.setProperty("transport.vfs.ContentType", "application/xml");
        properties.setProperty("transport.vfs.Build", "true");
        consumer = new VFSConsumer(properties, endpoint, environment, interval, "process", null, false, sequential) {
            @Override
            VfsSession newSession(long number) {
                VfsSession session = spy(super.newSession(number));
                doAnswer(call -> {
                    owners.put(call.getArgument(0), session);
                    return call.callRealMethod();
                }).when(session).enter(any(Thread.class));
                try {
                    setupSession.apply(session);
                } catch (FileSystemException problem) {
                    throw new AssertionError(problem);
                }
                return session;
            }
        };
        return environment;
    }

    @Test
    void firstPollWaitsForEndpointRegistrationBeforeConsumingSeed() throws Exception {
        Path seed = write("a.xml", "<a/>", 1);
        properties.setProperty("transport.vfs.ActionAfterFailure", "DELETE");
        SynapseEnvironment environment = deploy(folder.toUri().toString(), true);
        when(environment.getSynapseConfiguration()).thenReturn(null);
        consumer.poll();
        assertEquals("<a/>", Files.readString(seed));
        assertTrue(deliveries.isEmpty());
        assertEquals(0L, consumer.getHealth().get("filesDone"));
        assertEquals(0L, consumer.getHealth().get("filesFailed"));
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        InboundEndpoint registered = configuration.getInboundEndpoint(endpoint);
        when(configuration.getInboundEndpoint(endpoint)).thenReturn(null);
        consumer.poll();
        assertEquals("<a/>", Files.readString(seed));
        assertTrue(deliveries.isEmpty());
        when(configuration.getInboundEndpoint(endpoint)).thenReturn(registered);
        consumer.poll();
        consumer.poll();
        assertEquals(List.of("/a.xml"), deliveries);
        assertFalse(Files.exists(seed));
    }

    @Test
    void destroyedConsumerWarnsOnceAfterRemovalAndCannotTakeReplacementWork() throws Exception {
        deploy(folder.toUri().toString(), true);
        VFSConsumer old = consumer;
        old.destroy();
        when(configuration.getInboundEndpoint(endpoint)).thenReturn(null);
        Path seed = write("replacement.xml", "<replacement/>", 1);
        try (Logs logs = new Logs()) {
            old.poll();
            old.poll();
            assertEquals(1, logs.messages(Level.WARNING, endpoint, "polled while it is stopped").size());
            assertEquals("<replacement/>", Files.readString(seed));

            deploy(folder.toUri().toString(), true);
            old.poll();
            assertEquals("<replacement/>", Files.readString(seed));
            assertTrue(deliveries.isEmpty());
            consumer.poll();
            old.poll();
            consumer.poll();
            assertEquals(List.of("/replacement.xml"), deliveries);
            assertFalse(Files.exists(seed));
        }
    }

    @AfterEach
    void close() {
        if (consumer != null) {
            consumer.destroy();
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, endpoint);
            consumer = null;
        }
    }

    Path write(String name, String body, int age) throws Exception {
        Path path = folder.resolve(name);
        Files.createDirectories(path.getParent());
        Files.writeString(path, body);
        Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis() - age * 1000L));
        return path;
    }

    @Test
    void directoryCombinesFiltersCountAndInterval() throws Exception {
        properties.setProperty("transport.vfs.FileNamePattern", ".*");
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.FileSortAscending", "true");
        properties.setProperty("transport.vfs.FileSizeLimit", "4");
        properties.setProperty("transport.vfs.MinimumAge", "60");
        properties.setProperty("transport.vfs.MaximumAge", "120");
        properties.setProperty("transport.vfs.FileProcessCount", "2");
        properties.setProperty("transport.vfs.FileProcessInterval", "40");
        write("00.xml", "<large/>", 90);
        write("01.xml", "<x/>", 10);
        write("02.xml", "<x/>", 150);
        write("a.xml", "<a/>", 90);
        write("b.xml", "<b/>", 90);
        write("c.xml", "<c/>", 90);
        write("reserved.lock", "<r/>", 90);
        deploy(folder.toUri().toString(), true);
        long start = System.nanoTime();
        consumer.poll();
        assertEquals(List.of("/a.xml", "/b.xml"), deliveries);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 80);
        consumer.poll();
        assertEquals(List.of("/a.xml", "/b.xml", "/c.xml"), deliveries);
        assertTrue(Files.exists(folder.resolve("reserved.lock")));
    }

    // One poll of 1,000 files ran out of heap: the poll kept every task it ran, and each task its closed content. A
    // poll must let go of the content of each file it took, while it still takes the next ones, in both modes.
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void aPollKeepsNoContentOfTheFilesItAlreadyTook(boolean sequential) throws Exception {
        int files = 8;
        for (int i = 0; i < files; i++) {
            write("f" + i + ".xml", "<f" + i + "/>", 1);
        }
        if (!sequential) {
            properties.setProperty("transport.vfs.WorkerPoolSize", "2");
        }
        List<WeakReference<Object>> streams = Collections.synchronizedList(new ArrayList<>());
        ThreadLocal<Integer> mine = new ThreadLocal<>();
        // The builder keeps no reference to the stream: the message holds its text only.
        xmlBuilder = (in, type, message) -> {
            byte[] bytes;
            try {
                bytes = readAll(in);
            } catch (IOException failure) {
                throw new AxisFault("Test read failed.", failure);
            }
            synchronized (streams) {
                mine.set(streams.size());
                streams.add(new WeakReference<>(in));
            }
            OMElement element = OMAbstractFactory.getOMFactory().createOMElement(new QName("file"));
            element.setText(new String(bytes, StandardCharsets.UTF_8));
            return element;
        };
        AtomicReference<Boolean> released = new AtomicReference<>();
        AtomicInteger flows = new AtomicInteger();
        mediation = message -> {
            if (flows.incrementAndGet() == files) {
                // In the last flow, the poll still runs. Every other file is taken or ends its task now.
                List<WeakReference<Object>> others = new ArrayList<>(streams);
                others.remove((int) mine.get());
                try {
                    released.set(FileTaskTest.collected(others));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        deploy(folder.toUri().toString(), sequential);
        consumer.poll();
        assertEquals(files, deliveries.size());
        assertEquals(Boolean.TRUE, released.get(), "The poll kept the content of a file it already took.");
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        for (int count = in.read(buffer); count >= 0; count = in.read(buffer)) {
            out.write(buffer, 0, count);
        }
        return out.toByteArray();
    }

    // A type read of a listed FTP file made a parent of its own with a copy of the whole folder listing, and the poll
    // kept every listed file until its turn. The scan closes each listed file once it has read its type and details,
    // so a waiting file keeps no parent.
    @Test
    void theScanClosesEachListedFileBeforeTheFirstTake() throws Exception {
        for (int i = 0; i < 3; i++) {
            write("f" + i + ".xml", "<f" + i + "/>", 1);
        }
        String root = folder.toUri().toString();
        List<FileObject> listed = Collections.synchronizedList(new ArrayList<>());
        setupSession = session -> doAnswer(call -> {
            FileObject resolved = (FileObject) call.callRealMethod();
            String uri = call.getArgument(0);
            if (!uri.replaceAll("/+$", "").equals(root.replaceAll("/+$", ""))) {
                return resolved;
            }
            FileObject spied = spy(resolved);
            doAnswer(children -> {
                FileObject[] real = (FileObject[]) children.callRealMethod();
                FileObject[] watched = new FileObject[real.length];
                for (int i = 0; i < real.length; i++) {
                    watched[i] = spy(real[i]);
                    listed.add(watched[i]);
                }
                return watched;
            }).when(spied).getChildren();
            return spied;
        }).when(session).resolve(anyString(), any());
        AtomicReference<Boolean> closed = new AtomicReference<>();
        mediation = message -> {
            if (closed.get() == null) {
                closed.set(listed.size() == 3 && listed.stream().allMatch(file -> mockingDetails(file).getInvocations()
                        .stream().anyMatch(invocation -> invocation.getMethod().getName().equals("close"))));
            }
        };
        deploy(root, true);
        consumer.poll();
        assertEquals(3, deliveries.size());
        assertEquals(Boolean.TRUE, closed.get(), "A listed file was not closed before the first take.");
    }

    // An SFTP read took a channel of its own for each file: the read asked the type of the file while it held the
    // idle channel, so the stat opened another. A poll of many files must keep its channels, in both modes.
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void aPollOverSftpKeepsItsChannels(boolean sequential) throws Exception {
        int files = 20;
        Path root = folder.resolve("sftp");
        Path in = Files.createDirectories(root.resolve("in"));
        Path done = Files.createDirectories(root.resolve("done"));
        for (int i = 0; i < files; i++) {
            Files.write(in.resolve("f" + i + ".xml"), ("<f" + i + "/>").getBytes(StandardCharsets.UTF_8));
        }
        try (CountingSftpServer server = new CountingSftpServer(root, folder.resolve("hostkey.ser"))) {
            String base = "sftp://rig:rig@127.0.0.1:" + server.port();
            properties.setProperty("transport.vfs.ActionAfterProcess", "MOVE");
            properties.setProperty("transport.vfs.MoveAfterProcess", base + "/done");
            int workers = sequential ? 0 : 2;
            if (!sequential) {
                properties.setProperty("transport.vfs.WorkerPoolSize", String.valueOf(workers));
            }
            deploy(base + "/in", sequential);
            consumer.poll();
            assertEquals(files, deliveries.size());
            // The poll session and each worker session open their channel once. A channel per file is the fault.
            assertTrue(server.channels() <= 2 * (1 + workers), "SFTP channels opened: " + server.channels());
            // The move resolves its temporary names with no probe: 22.4 lookups per file, where 25.4 asked three
            // made up names of the target folder as well.
            assertTrue(server.lookups() <= 23 * files, "STAT and LSTAT requests: " + server.lookups());
            try (Stream<Path> left = Files.list(in)) {
                assertEquals(Collections.emptyList(), left.collect(Collectors.toList()));
            }
            try (Stream<Path> moved = Files.list(done)) {
                assertEquals(files, moved.count());
            }
        }
    }

    // An SFTP server on a folder that counts the SFTP channels it opened. It has the hard link extension of OpenSSH
    // that the lock of the inbound uses.
    static final class CountingSftpServer implements AutoCloseable {
        private final SshServer server = SshServer.setUpDefaultServer();
        private final AtomicInteger channels = new AtomicInteger();
        private final AtomicInteger lookups = new AtomicInteger();

        CountingSftpServer(Path root, Path hostKey) throws IOException {
            server.setHost("127.0.0.1");
            server.setPort(0);
            server.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(hostKey));
            server.setPasswordAuthenticator((user, password, session) -> user.equals(password));
            server.setFileSystemFactory(new VirtualFileSystemFactory(root));
            SftpSubsystemFactory sftp = new SftpSubsystemFactory();
            sftp.addSftpEventListener(new SftpEventListener() {
                @Override
                public void initialized(ServerSession session, int version) {
                    channels.incrementAndGet();
                }

                @Override
                public void received(ServerSession session, int type, int id) {
                    if (type == SftpConstants.SSH_FXP_STAT || type == SftpConstants.SSH_FXP_LSTAT) {
                        lookups.incrementAndGet();
                    }
                }
            });
            server.setSubsystemFactories(Collections.singletonList(sftp));
            server.start();
        }

        int port() {
            return server.getPort();
        }

        int channels() {
            return channels.get();
        }

        // The STAT and LSTAT requests the server answered.
        int lookups() {
            return lookups.get();
        }

        @Override
        public void close() throws IOException {
            server.stop(true);
        }
    }

    @Test
    void recursiveWorkersSkipTheTempFolderAndReportRelativePaths() throws Exception {
        properties.setProperty("transport.vfs.WorkerPoolSize", "2");
        properties.setProperty("transport.vfs.CheckSizeInterval", "10");
        write("sub/a.xml", "<a/>", 1);
        write("b.xml", "<b/>", 1);
        write(".part/internal.xml", "<t/>", 1);
        deploy(folder.toUri().toString() + "*", false);
        consumer.poll();
        assertEquals(2, deliveries.size());
        assertTrue(deliveries.containsAll(List.of("/sub/a.xml", "/b.xml")));
        assertTrue(threads.stream().allMatch(thread -> thread.startsWith("vfs-inbound-" + endpoint + "-worker-")));
        assertTrue(Files.exists(folder.resolve(".part/internal.xml")));
        assertFalse(Files.exists(folder.resolve("sub/a.xml")));
    }

    @Test
    void singleFileIsPolledAgainAfterAbsenceAndResume() throws Exception {
        Path file = folder.resolve("single.xml");
        deploy(file.toUri().toString(), false);
        consumer.poll();
        write("single.xml", "<a/>", 1);
        consumer.poll();
        assertFalse(Files.exists(file));
        consumer.pause();
        consumer.resume();
        write("single.xml", "<b/>", 1);
        consumer.poll();
        assertEquals(List.of("/single.xml", "/single.xml"), deliveries);
        assertFalse(Files.exists(file));
    }

    @Test
    void missingSequenceLeavesTheFileForTheNextPoll() throws Exception {
        Path file = write("one.xml", "<x/>", 1);
        deploy(folder.toUri().toString(), true);
        when(configuration.getSequence("process")).thenReturn(null);
        consumer.poll();
        assertTrue(Files.exists(file));
        assertTrue(deliveries.isEmpty());
        when(configuration.getSequence("process")).thenReturn(sequence);
        consumer.poll();
        assertEquals(List.of("/one.xml"), deliveries);
    }
    @Test
    void thirdPollSessionResetEndsAdmissionAndTheNextPollStartsFresh() throws Exception {
        resetDuringDelivery = true;
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.FileSortAscending", "true");
        for (int index = 0; index < 5; index++) {
            write("reset" + index + ".xml", "<r/>", 1);
        }
        deploy(folder.toUri().toString(), true);
        consumer.poll();
        assertEquals(List.of("/reset0.xml", "/reset1.xml", "/reset2.xml"), deliveries);
        assertFalse(Files.exists(folder.resolve("reset2.xml")));
        assertTrue(Files.exists(folder.resolve("reset3.xml")));
        consumer.poll();
        assertEquals(List.of("/reset0.xml", "/reset1.xml", "/reset2.xml", "/reset3.xml", "/reset4.xml"), deliveries);
    }

    @Test
    void workerSessionResetsDoNotSpendThePollSessionBudget() throws Exception {
        resetDuringDelivery = true;
        properties.setProperty("transport.vfs.WorkerPoolSize", "1");
        for (int index = 0; index < 5; index++) {
            write("worker" + index + ".xml", "<w/>", 1);
        }
        deploy(folder.toUri().toString(), false);
        consumer.poll();
        assertEquals(5, deliveries.size());
        for (int index = 0; index < 5; index++) {
            assertFalse(Files.exists(folder.resolve("worker" + index + ".xml")));
        }
    }

    @Test
    void aWorkerThatEndedWithAnErrorDoesNotStopThePoll() throws Exception {
        write("a.xml", "<a/>", 2);
        write("b.xml", "<b/>", 1);
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.WorkerPoolSize", "1");
        deploy(folder.toUri().toString(), false);
        mediation = message -> {
            Map<?, ?> headers = (Map<?, ?>) message.getAxis2MessageContext()
                    .getProperty(MessageContext.TRANSPORT_HEADERS);
            if ("a.xml".equals(headers.get(VFSConstants.FILE_NAME))) {
                throw new NoClassDefFoundError("Test worker error.");
            }
        };
        try (Logs logs = new Logs()) {
            consumer.poll();
            assertEquals(List.of("/a.xml", "/b.xml"), deliveries);
            assertTrue(Files.exists(folder.resolve("a.xml")));
            assertFalse(Files.exists(folder.resolve("b.xml")));
            assertTrue(logs.messages(Level.SEVERE, endpoint, "Poll failed").isEmpty());
            assertEquals(1, logs.messages(Level.SEVERE, endpoint, "A file worker failed").size());
        }
    }

    @Test
    void aCappedFileThatLeftTheListingLosesItsCountAndItsFlag() throws Exception {
        Path file = write("a.xml", "<a/>", 1);
        String uri = file.toUri().toString();
        properties.setProperty("transport.vfs.WorkerPoolSize", "1");
        properties.setProperty("transport.vfs.MaxReadTransportErrors", "1");
        String root = folder.toUri().toString();
        java.util.concurrent.atomic.AtomicBoolean metadataRefused = new java.util.concurrent.atomic.AtomicBoolean();
        setupSession = session -> {
            // Every read of the file content breaks the connection. The listing and the lock work.
            doAnswer(call -> {
                FileObject view = spy((FileObject) call.callRealMethod());
                FileSystemException lost = new FileSystemException("Test read connection lost.");
                lost.setFault(Fault.TRANSPORT);
                doThrow(lost).when(view).getContent();
                return view;
            }).when(session).resolve(eq(uri), any());
            // When asked, the listing shows the file but its metadata read fails with an application error.
            doAnswer(call -> {
                FileObject resolved = (FileObject) call.callRealMethod();
                if (!metadataRefused.get()) {
                    return resolved;
                }
                FileObject view = spy(resolved);
                doAnswer(list -> {
                    FileObject[] children = resolved.getChildren();
                    for (int index = 0; index < children.length; index++) {
                        if (children[index].getName().getBaseName().equals("a.xml")) {
                            FileObject child = spy(children[index]);
                            FileSystemException refused = new FileSystemException("Test metadata refused.");
                            refused.setFault(Fault.APPLICATION);
                            doThrow(refused).when(child).getContent();
                            children[index] = child;
                        }
                    }
                    return children;
                }).when(view).getChildren();
                return view;
            }).when(session).resolve(eq(root), any());
        };
        deploy(root, false);
        consumer.poll();
        assertEquals(0L, readCapFlags());
        consumer.poll();
        assertEquals(1L, readCapFlags());
        assertTrue(Files.exists(file));
        assertTrue(deliveries.isEmpty());
        // A file that is listed and skipped keeps its count.
        metadataRefused.set(true);
        consumer.poll();
        assertEquals(1L, readCapFlags());
        metadataRefused.set(false);
        Files.delete(file);
        consumer.poll();
        assertEquals(0L, readCapFlags());
    }

    private long readCapFlags() {
        Map<?, ?> flags = (Map<?, ?>) consumer.getHealth().get("flags");
        Map<?, ?> detail = flags == null ? null : (Map<?, ?>) flags.get("READ_CAP");
        return detail == null ? 0L : (Long) detail.get("count");
    }

    @Test
    void aMediationFailureAndAWorkerFailureAreLoggedWithTheirStackTrace() throws Exception {
        write("a.xml", "<a/>", 2);
        write("b.xml", "<b/>", 1);
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.WorkerPoolSize", "1");
        deploy(folder.toUri().toString(), false);
        mediation = message -> {
            Map<?, ?> headers = (Map<?, ?>) message.getAxis2MessageContext()
                    .getProperty(MessageContext.TRANSPORT_HEADERS);
            if ("a.xml".equals(headers.get(VFSConstants.FILE_NAME))) {
                throw new IllegalStateException();
            }
            throw new NoClassDefFoundError("Test worker error.");
        };
        try (Logs logs = new Logs()) {
            consumer.poll();
            List<String> mediated = logs.messages(Level.SEVERE, endpoint, "File mediation failed for ");
            assertEquals(1, mediated.size());
            assertTrue(mediated.get(0).contains("/a.xml."), mediated.get(0));
            assertTrue(mediated.get(0).contains("java.lang.IllegalStateException"), mediated.get(0));
            assertTrue(mediated.get(0).contains("\tat "), mediated.get(0));
            List<String> worker = logs.messages(Level.SEVERE, endpoint, "A file worker failed.");
            assertEquals(1, worker.size());
            assertTrue(worker.get(0).contains("java.lang.NoClassDefFoundError: Test worker error."), worker.get(0));
            assertTrue(worker.get(0).contains("\tat "), worker.get(0));
        }
    }

    // A temp name of a cut copy, as the shared module builds it: the target name, the creation second and a random
    // part, each 7 base 36 digits, and the suffix.
    private static String tempName(String target, long createdSeconds) {
        String created = Long.toString(createdSeconds, 36);
        return target + "." + "0000000".substring(created.length()) + created + "abc1234.part";
    }

    // The folders of a move setup: a success folder and a failure folder, with the files a sweep must judge.
    private Path moveSetup(Path out, Path failed) throws Exception {
        Path input = Files.createDirectory(folder.resolve("in"));
        Files.createDirectory(out);
        Files.createDirectory(failed);
        properties.setProperty("transport.vfs.ActionAfterProcess", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterProcess", out.toUri().toString());
        properties.setProperty("transport.vfs.ActionAfterFailure", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterFailure", failed.toUri().toString());
        mediation = message -> {
            Map<String, Object> headers = (Map<String, Object>) message.getAxis2MessageContext()
                    .getProperty(MessageContext.TRANSPORT_HEADERS);
            if (headers.get(VFSConstants.FILE_NAME).toString().startsWith("bad")) {
                headers.put(VFSConstants.ERROR_CODE, "1");
            }
        };
        return input;
    }

    @Test
    void moveTargetSweepRemovesOnlyAStaleTempFileOfAFolderThatThePollUsed() throws Exception {
        Path out = folder.resolve("out");
        Path failed = folder.resolve("failed");
        Path input = moveSetup(out, failed);
        long now = System.currentTimeMillis() / 1000;
        long old = now - TimeUnit.DAYS.toSeconds(2);
        Path stale = Files.writeString(out.resolve(tempName("a.xml", old)), "abc");
        Path young = Files.writeString(out.resolve(tempName("b.xml", now)), "abc");
        Path growing = Files.writeString(out.resolve(tempName("c.xml", old)), "abc");
        Path plain = Files.writeString(out.resolve("notes.txt"), "abc");
        Path shape = Files.writeString(out.resolve("d.xml.0000000.part"), "abc");
        Path unused = Files.writeString(failed.resolve(tempName("e.xml", old)), "abc");
        AtomicLong elapsed = new AtomicLong();
        try (MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime("cleanup")).thenAnswer(call ->
                    TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
            deploy(input.toUri().toString(), true);
            Files.writeString(input.resolve("good1.xml"), "<g/>");
            Files.writeString(input.resolve("bad1.xml"), "<b/>");
            consumer.poll();
            assertTrue(Files.isRegularFile(out.resolve("good1.xml")));
            assertTrue(Files.isRegularFile(failed.resolve("bad1.xml")));
            // The first sweep of each folder only notes the sizes.
            for (Path kept : Arrays.asList(stale, young, growing, plain, shape, unused)) {
                assertTrue(Files.exists(kept), kept.toString());
            }
            Files.writeString(growing, "abcdef");
            elapsed.set(TimeUnit.MINUTES.toMillis(61));
            Files.writeString(input.resolve("good2.xml"), "<g/>");
            consumer.poll();
            assertTrue(Files.isRegularFile(out.resolve("good2.xml")));
            assertFalse(Files.exists(stale), "A stale temp file with the same size an hour apart is removed");
            assertTrue(Files.exists(young), "A temp file younger than the stale age stays");
            assertTrue(Files.exists(growing), "A temp file whose size changed stays");
            assertTrue(Files.exists(plain), "A name that is not a temp name stays");
            assertTrue(Files.exists(shape), "A name with the wrong shape stays");
            assertTrue(Files.exists(unused), "A folder that this poll did not move into is not swept");
        } finally {
            close();
        }
    }

    @Test
    void moveTargetSweepWaitsWhileThePollSessionIsNotUsable() throws Exception {
        Path out = folder.resolve("out");
        Path input = moveSetup(out, folder.resolve("failed"));
        Path stale = Files.writeString(out.resolve(tempName("a.xml",
                System.currentTimeMillis() / 1000 - TimeUnit.DAYS.toSeconds(2))), "abc");
        AtomicLong elapsed = new AtomicLong();
        try (MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime("cleanup")).thenAnswer(call ->
                    TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
            properties.setProperty("transport.vfs.FileSortAttribute", "Name");
            deploy(input.toUri().toString(), true);
            Files.writeString(input.resolve("first.xml"), "<f/>");
            consumer.poll();
            elapsed.set(TimeUnit.MINUTES.toMillis(61));
            // Three session resets in one poll end its admission, so its session is not used for the sweep.
            resetDuringDelivery = true;
            for (int index = 0; index < 3; index++) {
                Files.writeString(input.resolve("reset" + index + ".xml"), "<r/>");
            }
            consumer.poll();
            assertTrue(Files.isRegularFile(out.resolve("reset2.xml")));
            assertTrue(Files.exists(stale), "No sweep with a session that spent its resets");
            resetDuringDelivery = false;
            Files.writeString(input.resolve("last.xml"), "<l/>");
            consumer.poll();
            assertFalse(Files.exists(stale), "The next usable poll sweeps the folder");
        } finally {
            close();
        }
    }

    @Test
    void failedMoveTargetSweepLeavesThePollAndTheMovesAndWarnsOnce() throws Exception {
        Path out = folder.resolve("out");
        Path input = moveSetup(out, folder.resolve("failed"));
        setupSession = session -> doThrow(new IllegalStateException("Sweep fixture"))
                .when(session).lockIo(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS);
        AtomicLong elapsed = new AtomicLong();
        try (Logs logs = new Logs(); MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime("cleanup")).thenAnswer(call ->
                    TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
            deploy(input.toUri().toString(), true);
            Files.writeString(input.resolve("one.xml"), "<o/>");
            consumer.poll();
            elapsed.set(TimeUnit.MINUTES.toMillis(61));
            Files.writeString(input.resolve("two.xml"), "<t/>");
            consumer.poll();
            assertEquals(List.of("/one.xml", "/two.xml"), deliveries);
            assertTrue(Files.isRegularFile(out.resolve("one.xml")));
            assertTrue(Files.isRegularFile(out.resolve("two.xml")));
            assertFalse(Files.exists(input.resolve("one.xml.fail")));
            assertFalse(Files.exists(input.resolve("two.xml.fail")));
            assertEquals(1, logs.messages(Level.WARNING, endpoint, "were not swept").size());
        } finally {
            close();
        }
    }

    private static final class Logs extends Handler implements AutoCloseable {
        final Logger root = Logger.getLogger("");
        final List<LogRecord> records = new ArrayList<>();

        Logs() {
            root.addHandler(this);
        }

        @Override public void publish(LogRecord record) { records.add(record); }
        @Override public void flush() { }
        @Override public void close() { root.removeHandler(this); }

        List<String> messages(Level level, String endpoint, String text) {
            List<String> matches = new ArrayList<>();
            for (LogRecord record : records) {
                String message = record.getMessage();
                if (record.getLevel().equals(level) && message.contains(endpoint) && message.contains(text)) {
                    matches.add(message);
                }
            }
            return matches;
        }
    }

    @Test
    void listingRuntimeReturnsNormallyAndErrorLeavesThePoll() throws Exception {
        for (boolean single : new boolean[]{false, true}) {
            Path file = write(single ? "single.xml" : "folder.xml", "<x/>", 1);
            String uri = (single ? file : folder).toUri().toString();
            AtomicReference<Throwable> next = new AtomicReference<>(new IllegalStateException("Listing fixture"));
            setupSession = session -> doAnswer(call -> {
                FileObject resolved = (FileObject) call.callRealMethod();
                Throwable thrown = next.getAndSet(null);
                if (thrown == null) {
                    return resolved;
                }
                FileObject view = spy(resolved);
                doAnswer(type -> { throw thrown; }).when(view).getType();
                return view;
            }).when(session).resolve(eq(uri), any());
            deploy(uri, true);
            try (Logs logs = new Logs()) {
                assertNull(consumer.poll());
                assertEquals(1, logs.messages(Level.SEVERE, endpoint, "Poll failed").size());
                assertTrue(logs.messages(Level.SEVERE, endpoint, "Poll failed").get(0)
                        .contains("java.lang.IllegalStateException: Listing fixture"));
                assertTrue(Files.exists(file));
                consumer.poll();
                assertFalse(Files.exists(file));
                next.set(new AssertionError("Listing error fixture"));
                assertThrows(AssertionError.class, consumer::poll);
            } finally {
                close();
            }
        }
    }

    // A listed name whose type read fails with a fault that is not transport, as the SMB2 share does for a name it
    // refuses. The other files of the folder are still delivered and the refused name stays untouched.
    @Test
    void refusedChildTypeSkipsOnlyThatFileAndTransportStillLeavesThePoll() throws Exception {
        Path first = write("a.xml", "<a/>", 1);
        Path refused = write("refused.xml", "<refused/>", 1);
        Path last = write("z.xml", "<z/>", 1);
        // A name that the server refuses has no file under that name, as on SMB2. Its fail marker, here in the
        // format of the old transport, must stay.
        Path marker = write("ghost.xml.fail", "1795000000000", 1);
        String uri = folder.toUri().toString();
        AtomicReference<Fault> fault = new AtomicReference<>(Fault.APPLICATION);
        setupSession = session -> doAnswer(call -> {
            FileObject resolved = (FileObject) call.callRealMethod();
            FileObject view = spy(resolved);
            doAnswer(list -> {
                FileObject[] children = resolved.getChildren();
                for (int index = 0; index < children.length; index++) {
                    if (children[index].getName().getBaseName().equals("refused.xml")) {
                        FileObject child = spy(children[index]);
                        FileSystemException problem = new FileSystemException("The type fixture refused the name.");
                        problem.setFault(fault.get());
                        doThrow(problem).when(child).getType();
                        children[index] = child;
                    }
                }
                FileObject ghost = spy(resolved.resolveFile("ghost.xml"));
                FileSystemException problem = new FileSystemException("The type fixture refused the name.");
                problem.setFault(fault.get());
                doThrow(problem).when(ghost).getType();
                FileObject[] listed = Arrays.copyOf(children, children.length + 1);
                listed[children.length] = ghost;
                return listed;
            }).when(view).getChildren();
            return view;
        }).when(session).resolve(eq(uri), any());
        deploy(uri, true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            consumer.poll();
            assertEquals(Set.of("/a.xml", "/z.xml"), new HashSet<>(deliveries));
            assertEquals(2, deliveries.size());
            assertFalse(Files.exists(first));
            assertFalse(Files.exists(last));
            assertEquals("<refused/>", Files.readString(refused));
            // The refused names stay in the listing, so the sweep does not take the marker as an orphan.
            assertTrue(Files.exists(marker));
            // The same names fail on each poll. The repeat limit of the endpoint log keeps one warning per name.
            List<String> warnings = logs.messages(Level.WARNING, endpoint, "Could not read the type of a listed file");
            assertEquals(2, warnings.size());
            assertEquals(1, warnings.stream().filter(line -> line.contains("refused.xml")).count());
            assertEquals(1, warnings.stream().filter(line -> line.contains("ghost.xml")).count());
        } finally {
            close();
        }

        deliveries.clear();
        Path waiting = write("c.xml", "<c/>", 1);
        fault.set(Fault.TRANSPORT);
        deploy(uri, true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            assertTrue(deliveries.isEmpty());
            assertEquals("<c/>", Files.readString(waiting));
            assertEquals("<refused/>", Files.readString(refused));
            assertTrue(logs.messages(Level.WARNING, endpoint, "Could not read the type of a listed file").isEmpty());
        }
    }

    // The listing gives the named file with a content whose read of the time, or of the size, fails with no
    // transport error, as on a server that sends no time or no size in its listing.
    void listedWithout(String uri, String name, boolean time) {
        setupSession = session -> doAnswer(call -> {
            FileObject resolved = (FileObject) call.callRealMethod();
            FileObject view = spy(resolved);
            doAnswer(list -> {
                FileObject[] children = resolved.getChildren();
                for (int index = 0; index < children.length; index++) {
                    if (children[index].getName().getBaseName().equals(name)) {
                        FileObject child = spy(children[index]);
                        FileContent content = spy(children[index].getContent());
                        if (time) {
                            doThrow(new FileSystemException("Last modification time not fetched."))
                                    .when(content).getLastModifiedTime();
                        } else {
                            doThrow(new FileSystemException("Size not fetched.")).when(content).getSize();
                        }
                        doReturn(content).when(child).getContent();
                        children[index] = child;
                    }
                }
                return children;
            }).when(view).getChildren();
            return view;
        }).when(session).resolve(eq(uri), any());
    }

    // Some SFTP servers send no time for any file in a listing. The old transport took such a file. A parameter that
    // needs the time skips the file with a line that names it. The header carries the time 0, as an FTP listing
    // with no time gives today.
    @Test
    void aListedFileWithNoTimeIsTakenUnlessAParameterNeedsTheTime() throws Exception {
        Path timeless = write("timeless.xml", "<timeless/>", 1);
        // The file has no time of its own either, so the identity of the file has an unknown time too.
        Files.setLastModifiedTime(timeless, FileTime.fromMillis(0));
        String uri = folder.toUri().toString();
        listedWithout(uri, "timeless.xml", true);
        List<Map<?, ?>> headers = Collections.synchronizedList(new ArrayList<>());
        mediation = message -> headers.add(new HashMap<>((Map<?, ?>) message.getAxis2MessageContext()
                .getProperty(MessageContext.TRANSPORT_HEADERS)));
        String[][] needing = {{VFSConstants.TRANSPORT_FILE_MINIMUM_AGE, "1"},
                {VFSConstants.TRANSPORT_FILE_MAXIMUM_AGE, "3600"},
                {VFSConstants.FILE_SORT_PARAM, VFSConstants.FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP}};
        for (String[] parameter : needing) {
            properties.setProperty(parameter[0], parameter[1]);
            deploy(uri, true);
            try (Logs logs = new Logs()) {
                consumer.poll();
                consumer.poll();
                assertTrue(deliveries.isEmpty(), parameter[0]);
                assertTrue(Files.exists(timeless));
                List<String> warnings = logs.messages(Level.WARNING, endpoint,
                        "does not give all the details of a file");
                assertEquals(1, warnings.size(), parameter[0]);
                assertTrue(warnings.get(0).contains(parameter[0] + " needs the modified time"), warnings.get(0));
                assertTrue(warnings.get(0).contains("timeless.xml"), warnings.get(0));
            } finally {
                close();
                properties.remove(parameter[0]);
            }
        }

        deploy(uri, true);
        try {
            consumer.poll();
            assertEquals(List.of("/timeless.xml"), deliveries);
            assertFalse(Files.exists(timeless));
            assertEquals(1, headers.size());
            assertEquals(0L, headers.get(0).get(VFSConstants.LAST_MODIFIED));
            assertEquals(11L, headers.get(0).get(VFSConstants.FILE_LENGTH));
        } finally {
            close();
        }
    }

    // The old transport failed a file whose size could not be read. Such a listed file is skipped, with a line that
    // names it.
    @Test
    void aListedFileWithNoSizeIsSkippedWithALine() throws Exception {
        Path sizeless = write("sizeless.xml", "<sizeless/>", 1);
        String uri = folder.toUri().toString();
        listedWithout(uri, "sizeless.xml", false);
        deploy(uri, true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            consumer.poll();
            assertTrue(deliveries.isEmpty());
            assertTrue(Files.exists(sizeless));
            List<String> warnings = logs.messages(Level.WARNING, endpoint, "does not give all the details of a file");
            assertEquals(1, warnings.size());
            assertTrue(warnings.get(0).contains("the inbound needs the size"), warnings.get(0));
            assertTrue(warnings.get(0).contains("sizeless.xml"), warnings.get(0));
        } finally {
            close();
        }
    }

    String undecodableWarning(boolean sftp) {
        return "[inbound:" + endpoint + "] A file name in file://" + folder + " holds a character that could not be"
                + " decoded as UTF-8. If such a file is never taken, the server uses another encoding for file names."
                + (sftp ? " Set fileNameEncoding in the address, for example fileNameEncoding=ISO-8859-1." : "");
    }

    // A client that can not decode a name lists it with the replacement character. A local file name can hold that
    // character itself, so the file is still taken, and the line names only the folder.
    @Test
    void undecodableNameWarnsOnceForTheFolderAndTheFileIsStillTaken() throws Exception {
        write("clean.xml", "<clean/>", 1);
        deploy(folder.toUri().toString(), true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            assertEquals(List.of("/clean.xml"), deliveries);
            assertTrue(logs.messages(Level.WARNING, endpoint, "could not be decoded").isEmpty());

            Path replaced = write("caf\uFFFD.xml", "<replaced/>", 1);
            consumer.poll();
            assertEquals(List.of("/clean.xml", "/caf\uFFFD.xml"), deliveries);
            assertFalse(Files.exists(replaced));
            assertEquals(List.of(undecodableWarning(false)),
                    logs.messages(Level.WARNING, endpoint, "could not be decoded"));
        }
    }

    // Each read of the repeat clock is six minutes after the one before, so the repeat limit holds no line back.
    @Test
    void manyUndecodableNamesInOneFolderGiveOneWarningPerPoll() throws Exception {
        Path first = write("one\uFFFD.xml", "<one/>", 1);
        Path second = write("two\uFFFD.xml", "<two/>", 1);
        write("plain.xml", "<plain/>", 1);
        deploy(folder.toUri().toString(), true);
        AtomicLong clock = new AtomicLong();
        try (Logs logs = new Logs();
                MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime("repeat")).thenAnswer(call ->
                    clock.addAndGet(TimeUnit.MINUTES.toNanos(6)));
            consumer.poll();
            assertEquals(Set.of("/one\uFFFD.xml", "/two\uFFFD.xml", "/plain.xml"), new HashSet<>(deliveries));
            assertEquals(3, deliveries.size());
            assertFalse(Files.exists(first));
            assertFalse(Files.exists(second));
            assertEquals(List.of(undecodableWarning(false)),
                    logs.messages(Level.WARNING, endpoint, "could not be decoded"));
        }
    }

    // Only the name of the listed folder reports the sftp scheme. The files are still read from the local folder.
    @Test
    void undecodableNameOnAnSftpFolderNamesTheFileNameEncodingOption() throws Exception {
        Path replaced = write("caf\uFFFD.xml", "<replaced/>", 1);
        String uri = folder.toUri().toString();
        setupSession = session -> doAnswer(call -> {
            FileObject resolved = (FileObject) call.callRealMethod();
            FileObject view = spy(resolved);
            FileName name = spy(resolved.getName());
            doReturn("sftp").when(name).getScheme();
            doReturn(resolved.getName().getURI()).when(name).getURI();
            doReturn(name).when(view).getName();
            doAnswer(list -> resolved.getChildren()).when(view).getChildren();
            return view;
        }).when(session).resolve(eq(uri), any());
        deploy(uri, true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            assertEquals(List.of("/caf\uFFFD.xml"), deliveries);
            assertFalse(Files.exists(replaced));
            assertEquals(List.of(undecodableWarning(true)),
                    logs.messages(Level.WARNING, endpoint, "could not be decoded"));
        }
    }

    @Test
    void reconnectWaitUsesTheParsedIntervalForEachResolve() throws Exception {
        String uri = folder.toUri().toString();
        String key = "transport.vfs.ReconnectTimeout";
        properties.setProperty("transport.vfs.MaxRetryCount", "2");
        String[] inputs = {null, "30", "30000", "999", "1000", "1000s", "500ms", "500"};
        long[] expected = {30000, 30000, 30000, 999000, 1000, 1000000, 500, 500000};
        String clock = "pollwait." + endpoint;
        for (int index = 0; index < inputs.length; index++) {
            String input = inputs[index];
            if (input != null) {
                properties.setProperty(key, input);
            }
            AtomicLong elapsed = new AtomicLong();
            List<Long> attempts = new ArrayList<>();
            setupSession = session -> doAnswer(call -> {
                attempts.add(elapsed.get());
                throw new FileSystemException("The resolve fixture is unavailable.");
            }).when(session).resolve(eq(uri), any());
            try (Logs logs = new Logs(); MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
                clocks.when(() -> TestPoints.nanoTime(clock)).thenAnswer(call ->
                        TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
                clocks.when(() -> TestPoints.slice(eq(clock), anyLong())).thenAnswer(call -> {
                    elapsed.addAndGet(call.getArgument(1));
                    return 1L;
                });
                deploy(uri, true);
                consumer.poll();
                assertEquals(List.of(0L, expected[index]), attempts);
                List<String> warnings = logs.messages(Level.WARNING, endpoint, key);
                boolean bare = input != null && input.matches("[0-9]+");
                assertEquals(bare ? 1 : 0, warnings.size());
                if (bare) {
                    assertTrue(warnings.get(0).contains(input));
                    assertTrue(warnings.get(0).contains("unit"));
                    assertTrue(warnings.get(0).contains(input + (Long.parseLong(input) < 1000 ? " s" : " ms")));
                }
            } finally {
                close();
            }
        }
        for (String invalid : List.of("abc", "-5", "10m")) {
            properties.setProperty(key, invalid);
            assertTrue(assertThrows(IllegalArgumentException.class, () -> deploy(uri, true)).getMessage().contains(key));
        }
    }

    // As in the old transport, each failed resolve of the poll root that is tried again gives one WARN. The address
    // in the line is masked.
    @Test
    void eachFailedResolveThatIsTriedAgainWarnsWithTheMaskedAddress() throws Exception {
        String uri = "sftp://reader:Secret123@sftp.example.invalid/in";
        properties.setProperty("transport.vfs.MaxRetryCount", "3");
        properties.setProperty("transport.vfs.ReconnectTimeout", "500ms");
        List<String> resolved = Collections.synchronizedList(new ArrayList<>());
        setupSession = session -> doAnswer(call -> {
            resolved.add(call.getArgument(0));
            throw new FileSystemException("The resolve fixture is unavailable.");
        }).when(session).resolve(anyString(), any());
        deploy(uri, true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            assertEquals(3, resolved.size());
            List<String> warnings = logs.messages(Level.WARNING, endpoint, "Failed to resolve the file URI");
            assertEquals(2, warnings.size());
            for (int attempt = 1; attempt <= 2; attempt++) {
                String line = warnings.get(attempt - 1);
                assertTrue(line.contains("Failed to resolve the file URI: sftp://reader:***@sftp.example.invalid/in,"
                        + " in attempt " + attempt + ", "), line);
                assertTrue(line.endsWith(" Retrying in 500 milliseconds."), line);
            }
            for (LogRecord record : logs.records) {
                assertFalse(String.valueOf(record.getMessage()).contains("Secret123"), record.getMessage());
            }
        } finally {
            close();
        }
    }

    @Test
    void destroyWakesThePendingReconnectWithoutASecondResolve() throws Exception {
        String uri = folder.toUri().toString();
        properties.setProperty("transport.vfs.MaxRetryCount", "2");
        properties.setProperty("transport.vfs.ReconnectTimeout", "30");
        CountDownLatch entered = new CountDownLatch(1);
        AtomicLong attempts = new AtomicLong();
        setupSession = session -> doAnswer(call -> {
            attempts.incrementAndGet();
            entered.countDown();
            throw new FileSystemException("The resolve fixture is unavailable.");
        }).when(session).resolve(eq(uri), any());
        deploy(uri, true);
        Thread poller = new Thread(consumer::poll);
        poller.start();
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Thread.sleep(1000);
            long start = System.nanoTime();
            consumer.destroy();
            poller.join(1500);
            assertFalse(poller.isAlive());
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1500);
            assertEquals(1L, attempts.get());
        } finally {
            consumer.destroy();
            poller.join(2000);
        }
    }

    @Test
    void lockedDecisionDefersAReplacementAndLeavesARetryOutsideTheCount() throws Exception {
        for (boolean pending : new boolean[]{false, true}) {
            for (boolean replacement : new boolean[]{false, true}) {
                verifyCount(pending, replacement, true);
            }
        }
        verifyCount(false, true, false);
    }

    private void verifyCount(boolean pending, boolean replacement, boolean sequential) throws Exception {
        Path input = Files.createDirectory(folder.resolve("input" + pending + replacement + sequential));
        Path unavailable = Files.writeString(input.resolveSibling(input.getFileName() + ".txt"), "ordinary file");
        Path first = Files.writeString(input.resolve("a.xml"), "<a/>");
        Path marker = input.resolve("a.xml.fail");
        java.util.concurrent.atomic.AtomicBoolean withholdWrite = new java.util.concurrent.atomic.AtomicBoolean();
        setupSession = session -> doAnswer(call -> {
            org.wso2.vfs.shared.lock.LockIo io = spy((org.wso2.vfs.shared.lock.LockIo) call.callRealMethod());
            doAnswer(look -> {
                String uri = look.getArgument(0);
                if (withholdWrite.get() && uri.contains("a.xml.fail.") && uri.endsWith(".next")) {
                    throw new FileSystemException("Local state write fixture");
                }
                return look.callRealMethod();
            }).when(io).resolve(anyString());
            return io;
        }).when(session).lockIo(anyString());
        properties.setProperty("transport.vfs.FileProcessCount", "1");
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.FileSortAscending", "true");
        properties.setProperty("transport.vfs.ActionAfterProcess", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterProcess", unavailable.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION, "0");
        deliveries.clear();
        deploy(input.toUri().toString(), sequential);
        try {
            consumer.poll();
            assertEquals(List.of("/a.xml"), deliveries);
            assertTrue(Files.isRegularFile(marker), "The first failed move must have a durable record.");
            if (replacement) {
                Files.writeString(first, "<replacement/>");
            }
            Files.writeString(input.resolve("b.xml"), "<b/>");
            withholdWrite.set(pending);
            deliveries.clear();
            consumer.poll();
            assertEquals(List.of("/b.xml"), deliveries);
            assertTrue(Files.isRegularFile(input.resolve("b.xml.fail")));
            if (replacement) {
                // The replacement is taken at the next poll, after its old record was removed in this one.
                assertFalse(Files.exists(marker));
                deliveries.clear();
                consumer.poll();
                assertEquals(List.of("/a.xml"), deliveries);
            } else {
                assertTrue(Files.exists(marker));
            }
            assertEquals(pending, Files.exists(input.resolve("a.xml.lock")));
        } finally {
            withholdWrite.set(false);
            close();
        }
    }


    @Test
    void aRecordThatTheLookMissesDefersTheFileOnASequentialInbound() throws Exception {
        Path input = Files.createDirectory(folder.resolve("missed"));
        Path hidden = Files.createDirectory(folder.resolve("hidden"));
        Path unavailable = Files.writeString(folder.resolve("missed.txt"), "ordinary file");
        Files.writeString(input.resolve("a.xml"), "<a/>");
        Path marker = input.resolve("a.xml.fail");
        AtomicBoolean hide = new AtomicBoolean();
        setupSession = session -> doAnswer(call -> {
            LockIo real = (LockIo) call.callRealMethod();
            LockIo io = spy(real);
            doAnswer(look -> {
                String uri = look.getArgument(0);
                if (hide.get() && uri.endsWith("/a.xml.fail")) {
                    // The look of a server that answers a generic failure twice: the record reads as not there.
                    return real.resolve(hidden.toUri() + "a.xml.fail");
                }
                return look.callRealMethod();
            }).when(io).resolve(anyString());
            return io;
        }).when(session).lockIo(anyString());
        properties.setProperty("transport.vfs.ActionAfterProcess", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterProcess", unavailable.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION, "0");
        deliveries.clear();
        deploy(input.toUri().toString(), true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            assertEquals(List.of("/a.xml"), deliveries);
            assertTrue(Files.isRegularFile(marker), "The failed move must have a durable record.");

            hide.set(true);
            deliveries.clear();
            consumer.poll();
            assertEquals(List.of(), deliveries, "A record that the look misses must not take the file again.");
            assertEquals(1, logs.messages(Level.WARNING, endpoint, "so the file is taken at the next poll.").size());
            assertTrue(Files.isRegularFile(marker));

            hide.set(false);
            consumer.poll();
            assertEquals(List.of(), deliveries, "The record is seen again, so the post action is retried.");
            assertTrue(Files.isRegularFile(marker));
        } finally {
            hide.set(false);
            close();
        }
    }

    @Test
    void aFileThatStopsIsNamedInItsLine() throws Exception {
        Path input = Files.createDirectory(folder.resolve("stopped"));
        Files.writeString(input.resolve("a.xml"), "<a/>");
        // A failure that is no file system error, at the look of the store after the lock is taken.
        setupSession = session -> doAnswer(call -> {
            LockIo io = spy((LockIo) call.callRealMethod());
            doAnswer(look -> {
                if (look.<String>getArgument(0).endsWith("/stopped/a.xml") && Arrays.stream(
                        new Throwable().getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("beginFile"))) {
                    throw new IllegalStateException("Stop fixture");
                }
                return look.callRealMethod();
            }).when(io).resolve(anyString());
            return io;
        }).when(session).lockIo(anyString());
        deploy(input.toUri().toString(), true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            List<String> lines = logs.messages(Level.WARNING, endpoint, "File processing stopped.");
            assertEquals(1, lines.size());
            assertTrue(lines.get(0).contains("/stopped/a.xml"), lines.get(0));
        } finally {
            close();
        }
    }

    @Test
    void anUploadCheckThatLosesTheConnectionNamesTheFile() throws Exception {
        Path input = Files.createDirectory(folder.resolve("uploading"));
        Files.writeString(input.resolve("a.xml"), "<a/>");
        AtomicBoolean lose = new AtomicBoolean(true);
        setupSession = session -> doAnswer(call -> {
            LockIo io = spy((LockIo) call.callRealMethod());
            doAnswer(look -> {
                if (look.<String>getArgument(0).endsWith("/uploading/a.xml") && lose.getAndSet(false)) {
                    FileSystemException lost = new FileSystemException("Test upload check connection lost.");
                    lost.setFault(Fault.TRANSPORT);
                    throw lost;
                }
                return look.callRealMethod();
            }).when(io).resolve(anyString());
            return io;
        }).when(session).lockIo(anyString());
        properties.setProperty(VFSConstants.TRANSPORT_CHECK_SIZE_INTERVAL, "1");
        deploy(input.toUri().toString(), true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            List<String> lines = logs.messages(Level.WARNING, endpoint, "The file was not taken in this poll.");
            assertEquals(1, lines.size());
            assertTrue(lines.get(0).contains("/uploading/a.xml"), lines.get(0));
            assertEquals(List.of(), deliveries);
        } finally {
            close();
        }
    }

    // The line of a reply that could not be written names the source file of the reply and never the reply address,
    // also for an address with a password that the deploy check takes and that a resolve reads as host and port.
    @ParameterizedTest
    @ValueSource(strings = {"local", "password", "noHeaders"})
    @SuppressWarnings("unchecked")
    void aReplyThatCanNotBeWrittenNamesItsSourceFileAndNotItsAddress(String scenario) throws Exception {
        Path input = Files.createDirectory(folder.resolve("asked"));
        Path source = Files.writeString(input.resolve("sample.xml"), "<receipt>body</receipt>");
        properties.setProperty(VFSConstants.REPLY_FILE_URI, scenario.equals("password")
                ? "sftp://replyuser:22?pa@127.0.0.1:1/out" : source.resolve("impossible").toUri().toString());
        List<Object> codes = new ArrayList<>();
        mediation = message -> {
            InboundResponseSender sender = (InboundResponseSender)
                    message.getProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER);
            MessageContext axis = message.getAxis2MessageContext();
            axis.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/xml");
            Map<String, Object> headers = (Map<String, Object>) axis.getProperty(MessageContext.TRANSPORT_HEADERS);
            if (scenario.equals("noHeaders")) {
                headers.remove("RELATIVE_PATH");
            }
            assertThrows(SynapseException.class, () -> sender.sendBack(message));
            codes.add(headers.get(org.apache.synapse.commons.vfs.VFSConstants.ERROR_CODE));
        };
        deploy(input.toUri().toString(), true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            List<String> lines = logs.messages(Level.SEVERE, endpoint, "Could not write the reply file");
            assertEquals(1, lines.size());
            String text = lines.get(0).substring(lines.get(0).indexOf("] ") + 2);
            assertEquals(scenario.equals("noHeaders") ? "Could not write the reply file."
                    : "Could not write the reply file for /sample.xml.", text);
        } finally {
            close();
        }
        assertEquals(List.of("Could not write the reply file."), codes);
    }

    @Test
    void skippedTicksPreserveTheLastObservationAndSuccessfulListingsAdvanceIt() throws Exception {
        String uri = folder.toUri().toString();
        AtomicLong elapsed = new AtomicLong();
        AtomicReference<FileSystemException> failure = new AtomicReference<>();
        AtomicLong resolves = new AtomicLong();
        setupSession = session -> doAnswer(call -> {
            resolves.incrementAndGet();
            FileSystemException local = failure.getAndSet(null);
            if (local != null) {
                throw local;
            }
            return call.callRealMethod();
        }).when(session).resolve(eq(uri), any());
        properties.setProperty("transport.vfs.MaxRetryCount", "1");
        String clock = "pollwait." + endpoint;
        try (MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime(clock)).thenAnswer(call -> TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
            deploy(uri, true);
            EndpointHealth health = VFSConsumerLifecycleTest.field(consumer, "health", EndpointHealth.class);
            when(configuration.getSequence("process")).thenReturn(null);
            consumer.poll();
            assertEquals(0L, sampleHealth(health).get("lastSuccess"));
            assertEquals(0L, resolves.get());
            when(configuration.getSequence("process")).thenReturn(sequence);
            consumer.poll();
            Map<String, Object> empty = sampleHealth(health);
            long emptyAt = ((Number) empty.get("lastSuccess")).longValue();
            assertTrue(emptyAt > 0);
            assertEquals(0L, empty.get("filesDone"));
            Path queued = write("waiting.xml", "<q/>", 1);
            failure.set(new FileSystemException(new java.net.SocketException("Local transport control")));
            consumer.poll();
            Map<String, Object> failed = sampleHealth(health);
            assertEquals("DOWN", failed.get("state"));
            assertEquals(emptyAt, failed.get("lastSuccess"));
            long attempted = resolves.get();
            consumer.poll();
            assertEquals(attempted, resolves.get());
            assertTrue(Files.isRegularFile(queued));
            assertEquals(failed, sampleHealth(health));
            when(configuration.getSequence("process")).thenReturn(null);
            elapsed.addAndGet(1001);
            consumer.poll();
            assertEquals(attempted, resolves.get());
            assertEquals(failed, sampleHealth(health));
            when(configuration.getSequence("process")).thenReturn(sequence);
            consumer.poll();
            Map<String,Object> recovered = sampleHealth(health);
            assertEquals("OK", recovered.get("state"));
            assertTrue(((Number) recovered.get("lastSuccess")).longValue() > emptyAt);
            assertEquals(1L, recovered.get("filesDone"));
            assertEquals(failed.get("filesFailed"), recovered.get("filesFailed"));
            assertEquals(failed.get("transportFaults"), recovered.get("transportFaults"));
            assertEquals(failed.get("lastError"), recovered.get("lastError"));
            assertEquals(List.of("/waiting.xml"), deliveries);
            assertFalse(Files.exists(queued));
        }
    }

    // After a poll that failed on the transport before its listing, the next poll connects only after a wait. The
    // first wait is the poll interval, each next wait doubles, and no wait is longer than 300 s.
    @Test
    void transportBackoffStartsAtTheIntervalAndNeverWaitsMoreThan300Seconds() throws Exception {
        String uri = folder.toUri().toString();
        properties.setProperty("transport.vfs.MaxRetryCount", "1");
        long[] intervals = {1000, 120000, 300000, 600000};
        long[][] waits = {
                {1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 300000, 300000},
                {120000, 240000, 300000, 300000},
                {300000, 300000},
                {300000, 300000}};
        String clock = "pollwait." + endpoint;
        for (int index = 0; index < intervals.length; index++) {
            interval = intervals[index];
            AtomicLong elapsed = new AtomicLong();
            AtomicLong resolves = new AtomicLong();
            setupSession = session -> doAnswer(call -> {
                resolves.incrementAndGet();
                throw new FileSystemException(new SocketException("Local transport control"));
            }).when(session).resolve(eq(uri), any());
            try (MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
                clocks.when(() -> TestPoints.nanoTime(clock)).thenAnswer(call ->
                        TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
                deploy(uri, true);
                consumer.poll();
                assertEquals(1L, resolves.get());
                for (int step = 0; step < waits[index].length; step++) {
                    long wait = waits[index][step];
                    String row = "Interval " + interval + " ms, wait " + (step + 1) + " of " + wait + " ms";
                    long failedAt = elapsed.get();
                    elapsed.set(failedAt + wait - 1);
                    consumer.poll();
                    assertEquals(step + 1L, resolves.get(), row + ", a poll before the wait is over.");
                    elapsed.set(failedAt + wait);
                    consumer.poll();
                    assertEquals(step + 2L, resolves.get(), row + ", the first poll after the wait.");
                }
            } finally {
                close();
            }
        }
    }


    @ParameterizedTest
    @ValueSource(strings = {"default", "parameter", "header", "file", "append", "missing", "denied", "otherThread"})
    @SuppressWarnings("unchecked")
    void respondOnThePollingPath(String scenario) throws Exception {
        Path input = Files.createDirectory(folder.resolve("inbox"));
        Path source = Files.write(input.resolve("sample.xml"), "<receipt>body</receipt>".getBytes(StandardCharsets.UTF_8));
        Path replies = Files.createDirectory(folder.resolve("replies"));
        String target = "response.xml";
        if (!scenario.equals("missing")) {
            Path location = replies;
            if (scenario.equals("file")) {
                target = "explicit.xml";
                location = replies.resolve(target);
            } else if (scenario.equals("denied")) {
                location = source.resolve("impossible");
            }
            properties.setProperty(VFSConstants.REPLY_FILE_URI, location.toUri().toString());
        }
        if (scenario.equals("parameter") || scenario.equals("header")) {
            properties.setProperty(VFSConstants.REPLY_FILE_NAME, "parameter.xml");
            target = scenario.equals("header") ? "header.xml" : "parameter.xml";
        }
        if (scenario.equals("append")) {
            properties.setProperty(VFSConstants.APPEND, "true");
            Files.write(replies.resolve(target), "prefix".getBytes(StandardCharsets.UTF_8));
        }
        boolean expectFailure = scenario.equals("missing") || scenario.equals("denied") || scenario.equals("otherThread");
        mediation = message -> {
            Map<?, ?> metadata = (Map<?, ?>) message.getProperty(SynapseConstants.STATISTICS_METADATA);
            assertEquals("file", metadata.get("protocol"));
            assertTrue(metadata.get("fileURI").toString().endsWith("/inbox/sample.xml"));
            InboundResponseSender sender = (InboundResponseSender)
                    message.getProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER);
            assertNotNull(sender);
            MessageContext axis = message.getAxis2MessageContext();
            axis.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/xml");
            Map<String, Object> headers = (Map<String, Object>) axis.getProperty(MessageContext.TRANSPORT_HEADERS);
            if (scenario.equals("header")) {
                headers.put(VFSConstants.REPLY_FILE_NAME, "header.xml");
            }
            if (scenario.equals("otherThread")) {
                java.util.concurrent.CompletableFuture.runAsync(() ->
                        assertThrows(SynapseException.class, () -> sender.sendBack(message))).join();
            } else if (expectFailure) {
                assertThrows(SynapseException.class, () -> sender.sendBack(message));
            } else {
                sender.sendBack(message);
            }
            assertEquals(expectFailure, headers.containsKey(org.apache.synapse.commons.vfs.VFSConstants.ERROR_CODE));
        };
        deploy(input.toUri().toString(), true);
        consumer.poll();
        assertEquals(1, deliveries.size());
        assertEquals(expectFailure, Files.exists(source));
        try (java.util.stream.Stream<Path> files = Files.walk(replies)) {
            assertEquals(expectFailure ? 0 : 1, files.filter(Files::isRegularFile).count());
        }
        if (!expectFailure) {
            String body = new String(Files.readAllBytes(replies.resolve(target)), StandardCharsets.UTF_8);
            assertTrue(body.contains("<receipt>body</receipt>"), body);
            assertEquals(scenario.equals("append"), body.startsWith("prefix"));
        }
        Map<String, Object> snapshot = consumer.getHealth();
        assertEquals(EndpointHealth.State.valueOf((String) snapshot.get("state")).status(), snapshot.get("status"));
        assertEquals(expectFailure ? 1L : 0L, snapshot.get("filesFailed"));
        assertEquals(expectFailure ? 0L : 1L, snapshot.get("filesDone"));
    }

    @ParameterizedTest
    @CsvSource({
            "nested/response.xml,nested/response.xml,false,false,false",
            "nested/response.xml,nested/response.xml,true,true,false",
            "how%3Fwhen.xml,how?when.xml,false,true,false",
            "how%3Fwhen.xml,how?when.xml,true,false,false",
            "sub/how%3Fwhen%2525.xml,sub/how?when%25.xml,false,false,false",
            "sub/how%3Fwhen%2525.xml,sub/how?when%25.xml,true,true,false",
            "how?when.xml,how,false,false,false",
            "sum%2525.xml,sum%25.xml,true,false,false",
            "nested/response.xml,nested/response.xml,false,true,true",
            "how%3Fwhen.xml,how?when.xml,true,true,true"})
    void replyTargetKeepsItsName(String supplied, String filename, boolean append, boolean header,
                                  boolean explicit) throws Exception {
        Path input = Files.createDirectory(folder.resolve("input"));
        Files.writeString(input.resolve("source.xml"), "<receipt>body</receipt>");
        Path replies = Files.createDirectory(folder.resolve("replies"));
        Path expected = replies.resolve(filename);
        Files.createDirectories(expected.getParent());
        Files.writeString(expected, "old");
        properties.setProperty(VFSConstants.REPLY_FILE_URI, (explicit ? expected : replies).toUri().toString());
        properties.setProperty(VFSConstants.REPLY_FILE_NAME, header ? "parameter.xml" : supplied);
        properties.setProperty(VFSConstants.APPEND, Boolean.toString(append));
        mediation = message -> {
            MessageContext axis = message.getAxis2MessageContext();
            axis.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/xml");
            if (header) {
                @SuppressWarnings("unchecked")
                Map<String, Object> headers = (Map<String, Object>) axis.getProperty(MessageContext.TRANSPORT_HEADERS);
                headers.put(VFSConstants.REPLY_FILE_NAME, supplied);
            }
            ((InboundResponseSender) message.getProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER))
                    .sendBack(message);
        };
        deploy(input.toUri().toString(), !append);
        consumer.poll();
        assertEquals(1L, consumer.getHealth().get("filesDone"));
        assertEquals(0L, consumer.getHealth().get("filesFailed"));
        String body = Files.readString(expected);
        assertEquals(append, body.startsWith("old"));
        assertTrue(body.contains("<receipt>body</receipt>"), body);
        try (java.util.stream.Stream<Path> files = Files.walk(replies)) {
            assertEquals(1L, files.filter(Files::isRegularFile).count());
        }
    }

    // A reply name that leaves the reply folder is refused, from the header and from the parameter, also with Windows
    // separators and with encoded separators and dots. An absolute name, a name with a scheme and a double encoded
    // separator are appended to the reply folder, so they are written inside it. Nothing is written
    // outside the reply folder. The reply folder lies two levels deep, so a name that escaped it would still land
    // inside the test folder.
    @ParameterizedTest
    @CsvSource({"../out.xml,true,refused", "../out.xml,false,refused", "../../out.xml,true,refused",
            "../../out.xml,false,refused", "nested/../../out.xml,true,refused",
            "nested/../../out.xml,false,refused", "..\\out.xml,true,refused", "..\\out.xml,false,refused",
            "..\\..\\out.xml,true,refused", "..\\..\\out.xml,false,refused",
            "nested\\..\\..\\out.xml,true,refused", "nested\\..\\..\\out.xml,false,refused",
            "..%2Fout.xml,true,refused", "..%2Fout.xml,false,refused", "..%5Cout.xml,true,refused",
            "..%5Cout.xml,false,refused", "%2e%2e/out.xml,true,refused", "%2e%2e/out.xml,false,refused",
            "..%252Fout.xml,true,inside", "..%252Fout.xml,false,inside", "ABSOLUTE,true,inside",
            "ABSOLUTE,false,inside", "SCHEME,true,inside", "SCHEME,false,inside"})
    void aReplyNameStaysInsideTheReplyFolder(String supplied, boolean header, String outcome) throws Exception {
        Path input = Files.createDirectory(folder.resolve("input"));
        Path source = Files.writeString(input.resolve("source.xml"), "<receipt>body</receipt>");
        Path replies = Files.createDirectories(folder.resolve("one").resolve("two").resolve("replies"));
        Path outside = Files.createDirectory(folder.resolve("outside"));
        boolean inside = outcome.equals("inside");
        String name = supplied.equals("ABSOLUTE") ? outside.resolve("out.xml").toString()
                : supplied.equals("SCHEME") ? outside.resolve("out.xml").toUri().toString() : supplied;
        properties.setProperty(VFSConstants.REPLY_FILE_URI, replies.toUri().toString());
        properties.setProperty(VFSConstants.REPLY_FILE_NAME, header ? "parameter.xml" : name);
        List<Boolean> refused = new ArrayList<>();
        mediation = message -> {
            MessageContext axis = message.getAxis2MessageContext();
            axis.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/xml");
            if (header) {
                @SuppressWarnings("unchecked")
                Map<String, Object> headers = (Map<String, Object>) axis.getProperty(MessageContext.TRANSPORT_HEADERS);
                headers.put(VFSConstants.REPLY_FILE_NAME, name);
            }
            try {
                ((InboundResponseSender) message.getProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER))
                        .sendBack(message);
                refused.add(false);
            } catch (SynapseException expected) {
                refused.add(true);
            }
        };
        deploy(input.toUri().toString(), true);
        consumer.poll();
        assertEquals(List.of(!inside), refused, name);
        List<Path> written;
        try (Stream<Path> files = Files.walk(folder)) {
            written = files.filter(Files::isRegularFile).filter(file -> !file.startsWith(input))
                    .collect(Collectors.toList());
        }
        if (inside) {
            assertEquals(1, written.size(), written.toString());
            assertTrue(written.get(0).startsWith(replies)
                    && written.get(0).getFileName().toString().endsWith("out.xml"), written.toString());
            // The reply is written, so the file is done and the action after process runs, DELETE here.
            assertEquals(1L, consumer.getHealth().get("filesDone"));
            assertFalse(Files.exists(source));
        } else {
            assertEquals(List.of(), written);
            // The refused reply fails the file, so the action after failure runs, NONE here: the file stays.
            assertEquals(1L, consumer.getHealth().get("filesFailed"));
            assertTrue(Files.exists(source));
        }
    }

    @Test
    void twoWorkersWaitForEachOtherOnOneFixedReplyName() throws Exception {
        Path input = Files.createDirectory(folder.resolve("input"));
        Files.writeString(input.resolve("a.xml"), "<receipt>a</receipt>");
        Files.writeString(input.resolve("b.xml"), "<receipt>b</receipt>");
        Path replies = Files.createDirectory(folder.resolve("replies"));
        properties.setProperty(VFSConstants.REPLY_FILE_URI, replies.toUri().toString());
        properties.setProperty("transport.vfs.WorkerPoolSize", "2");
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        // The reply of a.xml holds the lock of response.xml inside its write until b.xml has started its reply.
        ApplicationXMLFormatter held = new ApplicationXMLFormatter() {
            @Override
            public void writeTo(MessageContext context, OMOutputFormat format, OutputStream out, boolean preserve)
                    throws AxisFault {
                writing.countDown();
                try {
                    assertTrue(released.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    throw new AxisFault("Interrupted.", interrupted);
                }
                super.writeTo(context, format, out, preserve);
            }
        };
        mediation = message -> {
            MessageContext axis = message.getAxis2MessageContext();
            Map<?, ?> headers = (Map<?, ?>) axis.getProperty(MessageContext.TRANSPORT_HEADERS);
            InboundResponseSender sender = (InboundResponseSender)
                    message.getProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER);
            if ("a.xml".equals(headers.get(VFSConstants.FILE_NAME))) {
                axis.getConfigurationContext().getAxisConfiguration().addMessageFormatter("application/held", held);
                axis.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/held");
            } else {
                axis.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/xml");
                try {
                    assertTrue(writing.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    throw new AssertionError(interrupted);
                }
                new Thread(() -> {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    released.countDown();
                }).start();
            }
            sender.sendBack(message);
        };
        deploy(input.toUri().toString(), false);
        consumer.poll();
        assertEquals(2L, consumer.getHealth().get("filesDone"));
        assertEquals(0L, consumer.getHealth().get("filesFailed"));
        assertFalse(Files.exists(input.resolve("a.xml")));
        assertFalse(Files.exists(input.resolve("b.xml")));
        String body = Files.readString(replies.resolve("response.xml"));
        assertTrue(body.contains("<receipt>b</receipt>"), body);
    }

    private Map<String, Object> sampleHealth(EndpointHealth health) throws Exception {
        // Normally the health ticker publishes the state. Sample it now on the fixture clock.
        java.lang.reflect.Method tick = EndpointHealth.class.getDeclaredMethod("refresh", boolean.class);
        tick.setAccessible(true);
        tick.invoke(health, false);
        return health.snapshot();
    }

    @Test
    void ordinarySiblingNamesKeepIndependentWorkerRecords() throws Exception {
        Path input = Files.createDirectory(folder.resolve("in"));
        Path target = Files.writeString(folder.resolve("file-instead-of-folder"), "ordinary file");
        List<String> names = List.of("x.csv", "x.csv.next");
        for (String name : names) {
            Files.writeString(input.resolve(name), "<sample/>");
        }
        properties.setProperty("transport.vfs.WorkerPoolSize", "2");
        properties.setProperty("transport.vfs.ActionAfterProcess", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterProcess", target.toUri().toString());
        deploy(input.toUri().toString(), false);
        consumer.poll();
        consumer.poll();
        consumer.poll();
        for (String name : names) {
            assertEquals(1, Collections.frequency(deliveries, "/" + name));
            Path source = input.resolve(name);
            byte[] stored = Files.readAllBytes(input.resolve(name + ".fail"));
            FailState record = FailState.parse(stored, source.toUri().toString(), name);
            assertNotNull(record);
            assertEquals(name, record.name);
            assertTrue(Files.isRegularFile(source));
            assertFalse(Files.exists(input.resolve(name + ".lock")));
        }
        assertEquals(names.size(), deliveries.size());
    }

    @Test
    void configuredSingleFileKeepsItsPathEscapesAndOptionsThroughAdmission() throws Exception {
        for (boolean sequential : new boolean[] {true, false}) {
            for (String name : List.of("plain.xml", "x?y.xml", "x%3Fy.xml", "two words.xml")) {
                Path input = Files.createTempDirectory(folder, "single");
                Path target = Files.writeString(input.resolve(name), "<target/>");
                Path neighbor = Files.writeString(input.resolve("x"), "<neighbor/>");
                String uri = target.toUri() + "?transport.vfs.AvoidPermissionCheck=true";
                java.util.concurrent.CopyOnWriteArrayList<String> lookups = new java.util.concurrent.CopyOnWriteArrayList<>();
                setupSession = value -> doAnswer(call -> {
                    org.wso2.vfs.shared.lock.LockIo io = spy((org.wso2.vfs.shared.lock.LockIo) call.callRealMethod());
                    doAnswer(read -> {
                        lookups.add(read.getArgument(0));
                        return read.callRealMethod();
                    }).when(io).resolve(anyString());
                    return io;
                }).when(value).lockIo(anyString());
                deliveries.clear();
                deploy("vfs:" + uri, sequential);
                try {
                    consumer.poll();
                    assertEquals(1, deliveries.size(), name);
                    assertFalse(Files.exists(target), name);
                    assertEquals("<neighbor/>", Files.readString(neighbor), name);
                    assertTrue(lookups.contains(uri), "Original data URI must reach LockIo: " + name);
                    String lock = target.toUri() + ".lock?transport.vfs.AvoidPermissionCheck=true";
                    assertTrue(lookups.contains(lock), "Original path and options on its lock: " + name);
                    consumer.poll();
                    assertEquals(1, deliveries.size());
                } finally {
                    close();
                }
            }
        }
    }
}
