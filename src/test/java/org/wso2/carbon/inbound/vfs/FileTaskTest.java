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

import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundEndpoint;
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.provider.Fault;
import org.wso2.carbon.inbound.vfs.processor.MoveTargetSpec;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.carbon.inbound.vfs.reply.ReplySender;
import org.wso2.vfs.shared.error.Classification;
import org.wso2.vfs.shared.error.ErrorClassifier;
import org.mockito.MockedConstruction;
import org.mockito.stubbing.Answer;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.provider.GenericURLFileName;
import org.wso2.org.apache.commons.vfs2.provider.sftp.SftpFileObject;
import org.wso2.org.apache.commons.vfs2.provider.sftp.SftpFileSystem;
import org.wso2.org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.lock.LockIo;
import org.wso2.vfs.shared.failstate.FailState;
import org.wso2.vfs.shared.write.TempName;
import org.wso2.vfs.shared.write.TempNames;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.zip.CRC32;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileObject;
import org.wso2.org.apache.commons.vfs2.provider.MoveOptions;
import org.wso2.vfs.shared.failstate.ActionResult;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.FileTask;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.carbon.inbound.vfs.poll.Lifecycle;
import org.wso2.carbon.inbound.vfs.poll.ReadErrorCounter;
import org.wso2.carbon.inbound.vfs.poll.WorkerPool;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.health.HealthRegistry;
import org.wso2.vfs.shared.lock.LockOutcome;
import org.wso2.vfs.shared.lock.ShareLock;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class FileTaskTest {
    @TempDir Path folder;
    final String endpoint = "file" + UUID.randomUUID();
    final FileInjectHandler inject = mock(FileInjectHandler.class);
    final SequenceMediator sequence = mock(SequenceMediator.class);
    EndpointHealth health;
    VFSConfig config;
    Lifecycle life;
    Generation generation;
    FileTask.Services services;
    FailStateStore store;
    Path file;
    Path lock;
    String successAction = "DELETE";
    String failureAction = "NONE";
    long retryMillis = 30000;
    final Properties destinations = new Properties();
    // More endpoint parameters for one test.
    final Properties extra = new Properties();
    // The data file shows an address with a password, as a file on an SFTP server does.
    boolean remoteName;
    boolean controlledOwner;
    boolean controlledResets;
    boolean rejectNextReset;
    boolean failNextDelete;
    long failedResetCount;
    final java.util.concurrent.atomic.AtomicBoolean denyOwner = new java.util.concurrent.atomic.AtomicBoolean();
    final java.util.concurrent.atomic.AtomicBoolean foreignOwner = new java.util.concurrent.atomic.AtomicBoolean();

    @BeforeEach
    void setup() throws Exception {
        health = HealthRegistry.register(HealthRegistry.Kind.INBOUND, endpoint, 1000);
        file = Files.write(folder.resolve("message.txt"), new byte[]{21, 22});
        lock = folder.resolve("message.txt.lock");
        when(inject.sequence()).thenReturn(sequence);
    }

    void start(int workers, long stopMillis) {
        start(workers, stopMillis, inject);
    }

    void start(int workers, long stopMillis, FileInjectHandler handler) {
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FILE_ACTION_AFTER_PROCESS, successAction);
        properties.setProperty(VFSConstants.TRANSPORT_FILE_ACTION_AFTER_FAILURE, failureAction);
        properties.setProperty(VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION, Long.toString(retryMillis));
        properties.putAll(destinations);
        properties.putAll(extra);
        config = new VFSConfig(properties);
        life = new Lifecycle(endpoint, health, number -> Collections.singletonList(session(number)), () -> stopMillis,
                workers, this::session);
        ShareLock locks = ShareLock.create(endpoint, true, 60000, health);
        store = FailStateStore.forEndpoint(endpoint, config.getFailStateParameters(), locks, health);
        Map<String, MoveTargetSpec> targets = new HashMap<>();
        for (String key : destinations.stringPropertyNames()) {
            targets.put(key, MoveTargetSpec.from(key, destinations.getProperty(key), config, health.namedLog()));
        }
        services = new FileTask.Services(endpoint, config, locks, store, new ReadErrorCounter(3), handler,
                targets, health);
        generation = life.enterPoll();
    }

    VfsSession session(long number) {
        VfsSession session = new VfsSession(endpoint, number, config, 1000, health.namedLog());
        if (controlledResets) {
            return resetSession(session);
        }
        if (remoteName) {
            return remoteNameSession(session);
        }
        if (!controlledOwner) {
            return session;
        }
        VfsSession wrapped = spy(session);
        doAnswer(call -> {
            String parameter = call.getArgument(0);
            org.wso2.vfs.shared.lock.LockIo real = (org.wso2.vfs.shared.lock.LockIo) call.callRealMethod();
            if (!denyOwner.get() && !foreignOwner.get()) {
                return real;
            }
            org.wso2.vfs.shared.lock.LockIo observed = spy(real);
            doAnswer(lookup -> {
                String path = lookup.getArgument(0);
                if (path.endsWith(".lock")) {
                    if (denyOwner.compareAndSet(true, false)) {
                        org.wso2.org.apache.commons.vfs2.FileSystemException denied =
                                new org.wso2.org.apache.commons.vfs2.FileSystemException("Test owner read refused.");
                        denied.setFault(org.wso2.org.apache.commons.vfs2.provider.Fault.APPLICATION);
                        throw denied;
                    }
                    if (foreignOwner.get()) {
                        return real.resolve(folder.resolve("absent.lock").toUri().toString());
                    }
                }
                return real.resolve(path);
            }).when(observed).resolve(anyString());
            return observed;
        }).when(wrapped).lockIo(anyString());
        return wrapped;
    }

    private VfsSession remoteNameSession(VfsSession session) {
        VfsSession wrapped = spy(session);
        try {
            doAnswer(call -> {
                FileObject resolved = (FileObject) call.callRealMethod();
                if (!file.toUri().toString().equals(call.getArgument(0))) {
                    return resolved;
                }
                FileName shown = spy(resolved.getName());
                doReturn("sftp://rig:secret-word@host/in/message.txt").when(shown).getURI();
                FileObject named = spy(resolved);
                doReturn(shown).when(named).getName();
                return named;
            }).when(wrapped).resolve(anyString(), any());
        } catch (FileSystemException impossible) {
            throw new AssertionError(impossible);
        }
        return wrapped;
    }

    private VfsSession resetSession(VfsSession session) {
        VfsSession wrapped = spy(session);
        try {
            doAnswer(call -> {
                FileObject resolved = (FileObject) call.callRealMethod();
                if (!failNextDelete) {
                    return resolved;
                }
                FileObject controlled = spy(resolved);
                doAnswer(del -> {
                    failNextDelete = false;
                    FileSystemException lost = new FileSystemException("Test connection lost during deletion.");
                    lost.setFault(Fault.TRANSPORT);
                    throw lost;
                }).when(controlled).delete();
                return controlled;
            }).when(wrapped).resolve(anyString(), any());
        } catch (FileSystemException impossible) {
            throw new AssertionError(impossible);
        }
        doAnswer(call -> {
            if (rejectNextReset) {
                rejectNextReset = false;
                failedResetCount++;
                return false;
            }
            return call.callRealMethod();
        }).when(wrapped).reset(any(), any());
        doAnswer(call -> failedResetCount).when(wrapped).failedSourceResets();
        return wrapped;
    }

    FileTask admitted(Path path, String relative) {
        assertTrue(life.fileStarted(generation));
        FileTask task = new FileTask(services, generation, path.toUri().toString(), relative, true);
        assertEquals(LockOutcome.Kind.ACQUIRED, task.acquire(null).kind);
        return task;
    }

    @AfterEach
    void cleanup() {
        if (generation != null) {
            life.exitPoll(generation);
        }
        if (life != null) {
            life.stop("destroy");
        }
        HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, endpoint);
    }

    @Test
    void eachWorkerKeepsItsHeadersAndWaitsForItsMediationOutcome() throws Exception {
        start(2, 1000);
        CountDownLatch inside = new CountDownLatch(2);
        CountDownLatch end = new CountDownLatch(1);
        Map<String, Map<String, Object>> seen = new java.util.concurrent.ConcurrentHashMap<>();
        when(inject.invoke(same(sequence), eq(endpoint), anyMap(), any())).thenAnswer(call -> {
            Map<String, Object> headers = call.getArgument(2);
            try (InputStream stream = ((FileInjectHandler.Content) call.getArgument(3)).open()) {
                assertTrue(stream.read() > 0);
                while (stream.read() != -1) { }
            }
            String name = (String) headers.get(VFSConstants.FILE_NAME);
            assertTrue(Thread.currentThread().getName().startsWith("vfs-inbound-" + endpoint + "-worker-"));
            seen.put(name, new HashMap<>(headers));
            inside.countDown();
            assertTrue(end.await(10, TimeUnit.SECONDS));
            return name.equals("message.txt");
        });
        Path other = Files.write(Files.createDirectory(folder.resolve("sub")).resolve("other.txt"), new byte[]{7});
        WorkerPool.Slot first = generation.workerPool.reserve();
        CompletableFuture<FileTask.Outcome> done = first.submit(admitted(file, ""));
        WorkerPool.Slot second = generation.workerPool.reserve();
        CompletableFuture<FileTask.Outcome> failed = second.submit(admitted(other, "/sub"));
        try {
            assertTrue(inside.await(10, TimeUnit.SECONDS));
            assertTrue(Files.exists(file));
            assertTrue(Files.exists(other));
            assertTrue(Files.exists(lock));
            assertTrue(Files.exists(other.resolveSibling("other.txt.lock")));
            assertFalse(done.isDone());
            assertFalse(failed.isDone());
            assertEquals("", seen.get("message.txt").get("RELATIVE_PATH"));
            assertEquals("/sub", seen.get("other.txt").get("RELATIVE_PATH"));
        } finally {
            end.countDown();
        }
        assertEquals(FileTask.Outcome.PROCESSED, done.get(10, TimeUnit.SECONDS));
        assertEquals(FileTask.Outcome.FAILED, failed.get(10, TimeUnit.SECONDS));
        assertFalse(Files.exists(file));
        assertTrue(Files.exists(other));
        assertFalse(Files.exists(lock));
        assertFalse(Files.exists(other.resolveSibling("other.txt.lock")));
    }

    private byte[] marker(String origin, int attempts, Path staged) throws Exception {
        byte[] bytes = Files.readAllBytes(file);
        StringBuilder digest = new StringBuilder();
        for (byte value : MessageDigest.getInstance("SHA-256").digest(bytes)) {
            digest.append(String.format("%02x", value));
        }
        String body = "vfsfail=2\nname=message.txt\nwritten=1\nsize=" + bytes.length
                + "\nmtime=" + Files.getLastModifiedTime(file).toMillis() + "\nmtimeSource=COMMAND\ndigestAlg=SHA-256\ndigest="
                + digest + "\norigin=new\noutcome=SUCCESS\nfailed=MOVE\n"
                + (origin == null ? "" : "moveOrigin=" + origin + "\n")
                + "next=MOVE_AFTER_FAILED_MOVE\nfailedTarget=saved.csv\ntarget=saved.csv\n"
                + "step=AFTER_DELETE\nclear=false\ntemp=" + staged + "\ncommitted=false\nphase=UNCLEAR\nattempts="
                + attempts + "\ndue=1\nflagged=false\n";
        CRC32 checksum = new CRC32();
        checksum.update(body.getBytes(StandardCharsets.UTF_8));
        byte[] record = (body + String.format("end=%08x\n", checksum.getValue())).getBytes(StandardCharsets.UTF_8);
        assertNotNull(FailState.parse(record, file.toUri().toString(), file.getFileName().toString()));
        return record;
    }

    @Test
    void fallbackNamesRetainTheEncodedRelativeFolderAcrossSourceUriForms() {
        Properties props = new Properties();
        props.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        String parameter = VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE;
        String uri = folder.resolve("target").toUri().toString();
        props.setProperty(parameter, uri);
        MoveTargetSpec target = MoveTargetSpec.from(parameter, uri, new VFSConfig(props), health.namedLog());
        for (String root : new String[]{"file:/input/*", "/input/", "file:///input?option=true"}) {
            assertEquals("sub%20dir/records.csv", VFSConsumer.fallbackTarget(target, root,
                    "file:///input/sub%20dir/records.csv"));
        }
        assertEquals("item.txt", VFSConsumer.fallbackTarget(target, "sftp://user@host/upload/item.txt",
                "sftp://user@host/upload/item.txt"));
        assertThrows(IllegalArgumentException.class, () -> VFSConsumer.fallbackTarget(target,
                "sftp://user@host/upload/in", "sftp://user@host/upload/incorrect/item.txt"));
    }

    @Test
    void retryUsesTheRecordedRootAndPreservesAnAmbiguousOlderRecord() throws Exception {
        Path ordinary = Files.createDirectory(folder.resolve("normal"));
        Path fallback = Files.createDirectory(folder.resolve("fallback"));
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, ordinary.toUri().toString());
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE, fallback.toUri().toString());
        successAction = "MOVE";
        start(0, 1000);
        String[] origins = {"ORDINARY", "FALLBACK", null, null};
        for (int i = 0; i < origins.length; i++) {
            boolean ambiguous = i == 3;
            int attempts = i == 2 ? 0 : 1;
            Path previous = i == 1 ? fallback : ordinary;
            Path other = i == 1 ? ordinary : fallback;
            byte[] payload = {(byte) i, 11};
            byte[] sentinel = {77};
            Files.write(file, payload);
            Files.write(other.resolve("saved.csv"), sentinel);
            Files.deleteIfExists(previous.resolve("saved.csv"));
            TempName temp = TempNames.begin("saved.csv", ".part");
            Path staged = previous.resolve(temp.name());
            temp.end();
            Files.write(staged, payload);
            byte[] record = marker(origins[i], attempts, staged);
            Path canonical = file.resolveSibling("message.txt.fail");
            Files.write(canonical, record);
            FileTask.Outcome outcome = admitted(file, "").processOnPoll(null, true);
            assertEquals(ambiguous ? FileTask.Outcome.SKIPPED : FileTask.Outcome.RETRIED, outcome, "case " + i);
            assertArrayEquals(sentinel, Files.readAllBytes(other.resolve("saved.csv")));
            assertFalse(Files.exists(lock));
            if (ambiguous) {
                assertArrayEquals(record, Files.readAllBytes(canonical));
                assertArrayEquals(payload, Files.readAllBytes(file));
                assertArrayEquals(payload, Files.readAllBytes(staged));
                assertFalse(Files.exists(previous.resolve("saved.csv")));
            } else {
                assertFalse(Files.exists(file));
                assertFalse(Files.exists(canonical));
                assertFalse(Files.exists(staged));
                assertArrayEquals(payload, Files.readAllBytes(previous.resolve("saved.csv")));
            }
        }
        verify(inject, never()).invoke(any(), any(), anyMap(), any());
    }

    @Test
    void aFailedSourceResetHandsTheLockToPendingAndTheNextFileCanWriteAfterAFastReset() throws Exception {
        controlledResets = true;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenAnswer(call -> {
            try (InputStream input = ((FileInjectHandler.Content) call.getArgument(3)).open()) {
                while (input.read() != -1) { }
            }
            return true;
        });
        rejectNextReset = true;
        failNextDelete = true;
        assertEquals(FileTask.Outcome.STATE_PENDING, admitted(file, "").processOnPoll(null, false));
        assertTrue(Files.exists(file));
        assertTrue(Files.exists(lock));
        assertFalse(Files.exists(file.resolveSibling("message.txt.fail")));
        Path item = Files.write(folder.resolve("next.txt"), new byte[]{44});
        failNextDelete = true;
        assertEquals(FileTask.Outcome.STATE_WRITTEN, admitted(item, "").processOnPoll(null, false));
        assertTrue(Files.exists(item));
        assertFalse(Files.exists(item.resolveSibling("next.txt.lock")));
        Path marker = item.resolveSibling("next.txt.fail");
        byte[] prior = Files.readAllBytes(marker);
        FailState written = FailState.parse(prior, item.toUri().toString(), "next.txt");
        assertEquals(FailState.Action.DELETE, written.failed);
        assertNotNull(written.identity.digest());
        // Keep the identity and action and make this isolated retry due.
        String body = new String(prior, StandardCharsets.UTF_8).split("end=")[0].replaceFirst("(?m)^due=[0-9]+$", "due=1");
        CRC32 crc = new CRC32();
        crc.update(body.getBytes(StandardCharsets.UTF_8));
        byte[] due = (body + String.format("end=%08x\n", crc.getValue())).getBytes(StandardCharsets.UTF_8);
        Files.write(marker, due);
        rejectNextReset = true;
        failNextDelete = true;
        assertEquals(FileTask.Outcome.STATE_PENDING, admitted(item, "").processOnPoll(null, true));
        assertArrayEquals(due, Files.readAllBytes(marker));
        assertTrue(Files.exists(item));
        assertTrue(Files.exists(item.resolveSibling("next.txt.lock")));
        verify(inject, times(2)).invoke(any(), any(), anyMap(), any());
    }

    @Test
    void publishedFileMetadataPreservesOtherFieldsAndMasksCredentials() {
        Axis2MessageContext message = new Axis2MessageContext(new MessageContext(), null, null);
        Map<String, String> previous = Collections.singletonMap("route", "orders");
        message.setProperty(SynapseConstants.ANALYTICS_METADATA, previous);
        FileInjectHandler.setStatisticsMetadata(message, "sftp://rig:secret@host/files/a.xml");
        Map<?, ?> fields = (Map<?, ?>) message.getProperty("statisticsMetadata");
        assertEquals("sftp://rig:***@host/files/a.xml", fields.get("fileURI"));
        assertEquals("file", fields.get("protocol"));
        Map<?, ?> published = (Map<?, ?>) message.getProperty(SynapseConstants.ANALYTICS_METADATA);
        assertTrue(published instanceof HashMap);
        assertEquals(fields.get("fileURI"), published.get("fileURI"));
        assertEquals(fields.get("protocol"), published.get("protocol"));
        assertEquals("orders", published.get("route"));
        assertEquals(1, previous.size());
    }

    // The poll keeps its tasks until it ends, so a finished task must let go of its content. A closed SFTP stream
    // still refers to its channel and the channel's buffers, and one poll of 1,000 files ran out of heap that way.
    @Test
    void aFinishedTaskKeepsNeitherItsContentNorItsStream() throws Exception {
        List<WeakReference<Object>> read = new ArrayList<>();
        Properties props = new Properties();
        props.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        // A real handler and no mock: a mock keeps the arguments of its calls.
        FileInjectHandler reading = new FileInjectHandler("files", "onError", mock(SynapseEnvironment.class),
                new VFSConfig(props)) {
            @Override
            public SequenceMediator sequence() {
                return sequence;
            }

            @Override
            public boolean invoke(SequenceMediator seq, String name, Map<String, Object> headers, Content content) {
                try {
                    InputStream stream = content.open();
                    while (stream.read() >= 0) {
                        // The flow reads the whole file, the task closes the stream.
                    }
                    read.add(new WeakReference<>(content));
                    read.add(new WeakReference<>(stream));
                } catch (IOException failure) {
                    throw new AssertionError(failure);
                }
                return true;
            }
        };
        start(0, 1000, reading);
        FileTask task = admitted(file, "");
        assertEquals(FileTask.Outcome.PROCESSED, task.processOnPoll(null, false));
        assertEquals(2, read.size());
        assertTrue(collected(read), "The finished task kept its content or its stream.");
        // The task stays reachable up to here, as it does in the list of its poll.
        assertNotNull(task);
    }

    static boolean collected(List<WeakReference<Object>> references) throws InterruptedException {
        for (int i = 0; i < 100 && references.stream().anyMatch(reference -> reference.get() != null); i++) {
            System.gc();
            Thread.sleep(50);
        }
        return references.stream().allMatch(reference -> reference.get() == null);
    }

    @Test
    void realHandlerReturnsSynchronousFaultAndHeaderOutcomes() throws Exception {
        start(0, 1000);
        Properties props = new Properties();
        props.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        props.setProperty("transport.vfs.ContentType", "application/xml");
        props.setProperty("transport.vfs.Build", "true");
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SynapseConfiguration synapse = mock(SynapseConfiguration.class);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn("files");
        when(synapse.getInboundEndpoint(endpoint)).thenReturn(inbound);
        MessageContext axis = new MessageContext();
        AxisConfiguration configuration = new AxisConfiguration();
        configuration.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        axis.setConfigurationContext(new ConfigurationContext(configuration));
        Axis2MessageContext message = new Axis2MessageContext(axis, synapse, environment);
        when(environment.createMessageContext()).thenReturn(message);
        when(sequence.isInitialized()).thenReturn(true);
        FileInjectHandler handler = new FileInjectHandler("files", "onError", environment, new VFSConfig(props));
        byte[] payload = "<input>sample</input>".getBytes(StandardCharsets.UTF_8);
        Files.write(file, payload);
        String uri = file.toUri().toString();
        VfsSession session = generation.pollSession;
        FileObject object = session.resolve(uri, session.options(VFSConstants.TRANSPORT_FILE_FILE_URI));
        try {
            for (int i = 0; i < 4; i++) {
                final int scenario = i;
                Map<String, Object> headers = new HashMap<>();
                headers.put(VFSConstants.FILE_NAME, file.getFileName().toString());
                doAnswer(call -> {
                    assertSame(headers, axis.getProperty(MessageContext.TRANSPORT_HEADERS));
                    assertEquals("sample", message.getEnvelope().getBody().getFirstElement().getText());
                    if (scenario == 1) {
                        headers.put(org.apache.synapse.commons.vfs.VFSConstants.ERROR_CODE, "file read failed");
                    }
                    if (scenario == 2) {
                        throw new IllegalStateException("Test unhandled fault.");
                    }
                    return scenario != 3;
                }).when(environment).injectInbound(same(message), same(sequence), eq(true));
                FileIdentity identity = new FileIdentity(uri, payload.length, Files.getLastModifiedTime(file).toMillis(),
                        FileIdentity.TimeSource.COMMAND, null);
                try (FileInjectHandler.Content content = new FileInjectHandler.Content(object, identity, store, generation,
                        session, endpoint, mock(org.wso2.carbon.inbound.vfs.reply.ReplySender.class), health.namedLog())) {
                    assertEquals(scenario == 0, handler.invoke(sequence, endpoint, headers, content));
                    assertNull(content.readFailure());
                }
            }
        } finally {
            object.close();
        }
        verify(environment, times(4)).injectInbound(same(message), same(sequence), eq(true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void addressHeadersHoldThePasswordUnlessTheEndpointMasksIt(boolean mask) throws Exception {
        remoteName = true;
        extra.setProperty(VFSConstants.REPLY_FILE_URI, "sftp://reply:secret-word@127.0.0.1:1/replies");
        extra.setProperty(VFSConstants.TRANSPORT_MASK_HEADER_PASSWORD, String.valueOf(mask));
        start(0, 1000);
        AtomicReference<Map<String, Object>> seen = new AtomicReference<>();
        when(inject.invoke(any(), any(), anyMap(), any())).thenAnswer(call -> {
            seen.set(new HashMap<>(call.<Map<String, Object>>getArgument(2)));
            return true;
        });
        assertEquals(FileTask.Outcome.PROCESSED, admitted(file, "").processOnPoll(null, false));
        // By default both addresses hold the password, as the transport has them.
        assertEquals(mask ? "sftp://rig:***@host/in/message.txt" : "sftp://rig:secret-word@host/in/message.txt",
                seen.get().get(VFSConstants.FILE_URI));
        assertEquals(mask ? "sftp://reply:***@127.0.0.1:1/replies" : "sftp://reply:secret-word@127.0.0.1:1/replies",
                seen.get().get(VFSConstants.REPLY_FILE_URI));
    }

    @Test
    @Tag("testbuild")
    void deleteWaitsAfterTheOwnerCheckUntilTheRelease(@TempDir Path root) throws Exception {
        String previous = System.setProperty("vfs.testpoints.dir", root.toString());
        String point = "filetask.step.10";
        try {
            start(0, 1000);
            when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
            TestPoints.copy("inbound", VFSConsumer.class);
            TestPoints.event("hold.ready", endpoint);
            Path copy;
            try (Stream<Path> folders = Files.list(root)) {
                copy = folders.filter(Files::isDirectory).findFirst().get();
            }
            Path hold = Files.write(copy.resolve(point + ".hold"),
                    Arrays.asList("endpoint=" + endpoint, "wait=30"), StandardCharsets.UTF_8);
            AtomicBoolean done = new AtomicBoolean();
            AtomicReference<String> seen = new AtomicReference<>("The task did not reach " + point);
            // The task runs on the thread of the poll. This thread watches the hold and releases it.
            Thread releaser = new Thread(() -> {
                try {
                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (System.nanoTime() < end && !reached(copy, point)) {
                        if (done.get()) {
                            seen.set("The task completed without waiting at " + point);
                            return;
                        }
                        Thread.sleep(5);
                    }
                    if (!reached(copy, point)) {
                        return;
                    }
                    Thread.sleep(300);
                    seen.set("waits=" + !done.get() + " file=" + Files.exists(file) + " lock=" + Files.exists(lock));
                    Files.delete(hold);
                } catch (Exception e) {
                    seen.set(e.toString());
                }
            });
            releaser.start();
            FileTask.Outcome outcome = admitted(file, "").processOnPoll(null, false);
            done.set(true);
            releaser.join(TimeUnit.SECONDS.toMillis(15));
            // At the hold the owner check is done and the delete is not: the file and its lock are still there.
            assertEquals("waits=true file=true lock=true", seen.get());
            assertEquals(FileTask.Outcome.PROCESSED, outcome);
            assertFalse(Files.exists(file));
            assertFalse(Files.exists(lock));
            assertTrue(Files.readAllLines(copy.resolve("events.log")).stream().anyMatch(line ->
                    line.contains("hold-end") && line.contains("at=" + point) && line.contains("end=released")));
        } finally {
            if (previous == null) {
                System.clearProperty("vfs.testpoints.dir");
            } else {
                System.setProperty("vfs.testpoints.dir", previous);
            }
        }
    }

    private static boolean reached(Path copy, String point) throws java.io.IOException {
        try (Stream<Path> names = Files.list(copy)) {
            return names.map(name -> name.getFileName().toString())
                    .anyMatch(name -> name.startsWith(point + "@") && name.endsWith(".reached"));
        }
    }

    @Test
    void aLostOwnerCannotRunThePostActionOrReleaseTheLock() throws Exception {
        controlledOwner = true;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenAnswer(call -> {
            foreignOwner.set(true);
            return true;
        });
        FileTask task = admitted(file, "");
        byte[] lockBefore = Files.readAllBytes(lock);
        assertEquals(FileTask.Outcome.SKIPPED, task.processOnPoll(null, false));
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(file));
        assertArrayEquals(lockBefore, Files.readAllBytes(lock));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
    }

    @Test
    void failedOwnerCheckWithNoActionPreservesBothMediationOutcomes() throws Exception {
        controlledOwner = true;
        successAction = "NONE";
        start(0, 1000);
        for (boolean success : new boolean[]{true, false}) {
            doAnswer(call -> {
                denyOwner.set(true);
                return success;
            }).when(inject).invoke(any(), any(), anyMap(), any());
            FileTask task = admitted(file, "");
            assertEquals(success ? FileTask.Outcome.PROCESSED : FileTask.Outcome.FAILED, task.processOnPoll(null, false));
            assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(file));
            assertFalse(Files.exists(folder.resolve("message.txt.fail")));
            assertFalse(Files.exists(lock));
        }
        verify(inject, times(2)).invoke(any(), any(), anyMap(), any());
    }

    @Test
    void pollResolveAfterResetUsesTheCurrentRootAndSkipsTheListedObject() throws Exception {
        start(0, 1000);
        VfsSession session = generation.pollSession;
        FileObject listed = spy(session.resolve(file.toUri().toString(), session.options(VFSConstants.TRANSPORT_FILE_FILE_URI)));
        assertTrue(session.reset(listed.getFileSystem(), null));
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
        assertEquals(FileTask.Outcome.PROCESSED, admitted(file, "").processOnPoll(listed, false));
        verify(listed, never()).refresh();
        assertFalse(Files.exists(file));
        assertFalse(Files.exists(lock));
        listed.close();
    }

    @Test
    void retiredPollCancelsBeforeTheFirstLockCall() {
        start(0, 0);
        FileTask waiting = new FileTask(services, generation, file.toUri().toString(), "", true);
        life.exitPoll(generation);
        life.stop("pause");
        generation = null;
        assertThrows(java.util.concurrent.CancellationException.class, () -> waiting.acquire(null));
        assertTrue(Files.exists(file));
        assertFalse(Files.exists(lock));
        verifyNoInteractions(inject);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void postMediationExceptionRetriesOnlyTheSelectedMove(boolean success) throws Exception {
        Path done = Files.createDirectory(folder.resolve("done"));
        Path failed = Files.createDirectory(folder.resolve("failed"));
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, done.toUri().toString());
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILURE, failed.toUri().toString());
        successAction = "MOVE";
        failureAction = "MOVE";
        controlledOwner = true;
        retryMillis = 0;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenAnswer(call -> {
            doThrow(new IllegalStateException("Local owner lookup failed."))
                    .doCallRealMethod().when(generation.pollSession).lockIo(VFSConstants.TRANSPORT_FILE_FILE_URI);
            return success;
        });
        assertEquals(FileTask.Outcome.STATE_WRITTEN, admitted(file, "").processOnPoll(null, false));
        Path marker = folder.resolve("message.txt.fail");
        FailState state = FailState.parse(Files.readAllBytes(marker), file.toUri().toString(), "message.txt");
        assertNotNull(state);
        assertEquals(success ? FailState.Outcome.SUCCESS : FailState.Outcome.FAILURE, state.outcome);
        assertEquals(FailState.Action.MOVE, state.failed);
        assertEquals(FailState.MoveOrigin.ORDINARY, state.moveOrigin);
        assertEquals("message.txt", state.failedTarget);
        assertEquals(ActionResult.Phase.BEFORE_ACTION, state.action.phase);
        assertFalse(Files.exists(lock));
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(file));
        assertEquals(FileTask.Outcome.RETRIED, admitted(file, "").processOnPoll(null, false));
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes((success ? done : failed).resolve("message.txt")));
        assertFalse(Files.exists((success ? failed : done).resolve("message.txt")));
        assertFalse(Files.exists(marker));
        assertFalse(Files.exists(file));
        assertFalse(Files.exists(lock));
        verify(inject, times(1)).invoke(any(), any(), anyMap(), any());
        assertEquals(success ? 1L : 0L, health.snapshot().get("filesDone"));
        assertEquals(success ? 0L : 1L, health.snapshot().get("filesFailed"));
    }

    // The first failure of a post action is named at WARN with the file, the step and the error. Its retries are not.
    @Test
    void theFirstFailureOfAPostActionIsAWarningAndItsRetryIsNot() throws Exception {
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, folder.resolve("absent").toUri().toString());
        successAction = "MOVE";
        retryMillis = 0;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
        Logger logger = Logger.getLogger(EndpointHealth.class.getName());
        List<String> warnings = new ArrayList<>();
        Handler handler = new Handler() {
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING && record.getMessage().contains("The post action")) {
                    warnings.add(record.getMessage());
                }
            }
            public void flush() { }
            public void close() { }
        };
        logger.addHandler(handler);
        try {
            assertEquals(FileTask.Outcome.STATE_WRITTEN, admitted(file, "").processOnPoll(null, false));
            assertEquals(1, warnings.size(), warnings.toString());
            assertTrue(warnings.get(0).contains("The post action MOVE failed at step") && warnings.get(0).contains("message.txt")
                    && warnings.get(0).contains("move folder is unavailable"), warnings.toString());
            admitted(file, "").processOnPoll(null, false);
            assertEquals(1, warnings.size(), warnings.toString());
            assertTrue(Files.exists(file));
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void moveThatCompletedBeforeAnUncheckedReturnIsNotDeliveredAgain() throws Exception {
        Path done = Files.createDirectory(folder.resolve("done"));
        Path target = done.resolve("message.txt");
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, done.toUri().toString());
        successAction = "MOVE";
        controlledOwner = true;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
        doAnswer(call -> {
            call.callRealMethod();
            if (Files.exists(target) && !Files.exists(file)) {
                throw new IllegalStateException("Local move completion callback failed.");
            }
            return null;
        }).when(generation.pollSession).remoteCallEnded();
        assertEquals(FileTask.Outcome.STATE_NOT_NEEDED, admitted(file, "").processOnPoll(null, false));
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(target));
        assertFalse(Files.exists(file));
        assertFalse(Files.exists(lock));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
        FileTask next = new FileTask(services, generation, file.toUri().toString(), "", true);
        assertEquals(LockOutcome.Kind.FILE_GONE, next.acquire(null).kind);
        verify(inject, times(1)).invoke(any(), any(), anyMap(), any());
        assertEquals(1L, health.snapshot().get("filesDone"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void uncertainMoveComparesTheRecordedTargetBeforeRetry(boolean matches) throws Exception {
        Path done = Files.createDirectory(folder.resolve("done"));
        Path target = done.resolve("message.txt");
        byte[] original = Files.readAllBytes(file);
        Files.write(target, new byte[]{10, 11});
        java.nio.file.attribute.FileTime targetTime = java.nio.file.attribute.FileTime.fromMillis(120000);
        Files.setLastModifiedTime(target, targetTime);
        String destination = done.toUri().toString();
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, destination);
        successAction = "MOVE";
        retryMillis = 0;
        controlledOwner = true;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
        VfsSession session = generation.pollSession;
        String uri = file.toUri().toString();
        AbstractFileObject<?> source = spy((AbstractFileObject<?>) session.resolve(uri,
                session.options(VFSConstants.TRANSPORT_FILE_FILE_URI)));
        doAnswer(call -> {
            if (matches) {
                Files.write(target, original);
                Files.setLastModifiedTime(target, targetTime);
            }
            throw new IllegalStateException("Local move returned no result.");
        }).when(source).moveTo(any(FileObject.class), any(MoveOptions.class));
        doReturn(source).doCallRealMethod().when(session).resolve(eq(uri), any());
        assertEquals(FileTask.Outcome.STATE_WRITTEN, admitted(file, "").processOnPoll(null, false));
        Path marker = folder.resolve("message.txt.fail");
        FailState state = FailState.parse(Files.readAllBytes(marker), file.toUri().toString(), "message.txt");
        assertNotNull(state);
        assertEquals(FailState.Outcome.SUCCESS, state.outcome);
        assertEquals(ActionResult.Phase.UNCLEAR, state.action.phase);
        assertFalse(state.action.clear);
        assertFalse(state.action.committed);
        assertEquals("message.txt", state.failedTarget);
        assertEquals(FileTask.Outcome.RETRIED, admitted(file, "").processOnPoll(null, false));
        assertArrayEquals(original, Files.readAllBytes(target));
        assertEquals(matches, targetTime.equals(Files.getLastModifiedTime(target)));
        assertFalse(Files.exists(marker));
        assertFalse(Files.exists(file));
        assertFalse(Files.exists(lock));
        verify(inject, times(1)).invoke(any(), any(), anyMap(), any());
    }

    @Test
    void aPostActionThatStopsIsNamedInItsLine() throws Exception {
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS,
                folder.resolve("notCreatedYet").toUri().toString());
        successAction = "MOVE";
        controlledOwner = true;
        retryMillis = 0;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
        doReturn(0L).doThrow(new IllegalStateException("Local accounting failed."))
                .doCallRealMethod().when(generation.pollSession).failedSourceResets();
        Logger logger = Logger.getLogger(EndpointHealth.class.getName());
        List<String> warnings = new ArrayList<>();
        Handler handler = new Handler() {
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING && record.getMessage().contains("File processing stopped.")) {
                    warnings.add(record.getMessage());
                }
            }
            public void flush() { }
            public void close() { }
        };
        logger.addHandler(handler);
        try {
            assertEquals(FileTask.Outcome.STATE_PENDING, admitted(file, "").processOnPoll(null, false));
            assertEquals(1, warnings.size(), warnings.toString());
            assertTrue(warnings.get(0).contains(file.toUri().toString()), warnings.toString());
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void interruptedAccountingTransfersTheKnownMoveFailureToPendingOnce() throws Exception {
        Path done = folder.resolve("notCreatedYet");
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, done.toUri().toString());
        successAction = "MOVE";
        controlledOwner = true;
        retryMillis = 0;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
        VfsSession session = generation.pollSession;
        doReturn(0L).doThrow(new IllegalStateException("Local accounting failed."))
                .doCallRealMethod().when(session).failedSourceResets();
        assertEquals(FileTask.Outcome.STATE_PENDING, admitted(file, "").processOnPoll(null, false));
        Path marker = folder.resolve("message.txt.fail");
        assertFalse(Files.exists(marker));
        assertTrue(Files.exists(lock));
        FileTask next = new FileTask(services, generation, file.toUri().toString(), "", true);
        assertEquals(LockOutcome.Kind.BUSY_LOCAL, next.acquire(null).kind);
        FailStateStore.PendingResult pending = store.runPending(session.lockIo(VFSConstants.TRANSPORT_FILE_FILE_URI));
        assertEquals(1, pending.written);
        assertEquals(0, pending.pending);
        assertFalse(Files.exists(lock));
        FailState state = FailState.parse(Files.readAllBytes(marker), file.toUri().toString(), "message.txt");
        assertNotNull(state);
        assertEquals(FailState.Outcome.SUCCESS, state.outcome);
        assertEquals(FailState.Action.MOVE, state.failed);
        assertEquals("message.txt", state.failedTarget);
        assertTrue(state.action.clear);
        Files.createDirectory(done);
        assertEquals(FileTask.Outcome.RETRIED, admitted(file, "").processOnPoll(null, false));
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(done.resolve("message.txt")));
        assertFalse(Files.exists(file));
        assertFalse(Files.exists(marker));
        assertFalse(Files.exists(lock));
        verify(inject, times(1)).invoke(any(), any(), anyMap(), any());
        assertEquals(1L, health.snapshot().get("filesDone"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"self", "parentAlias", "otherLink", "separate", "differentBytes", "otherHost", "otherPort",
            "numericAlias", "timeoutOnly", "unknown", "opaque", "remoteSeparatePath", "confirmed"})
    void moveRecoveryChoosesItsActionWithoutChangingEitherEntry(String scenario) throws Exception {
        boolean self = scenario.equals("self");
        boolean alias = scenario.equals("parentAlias");
        boolean link = scenario.equals("otherLink");
        boolean unknown = scenario.equals("unknown") || scenario.equals("opaque");
        boolean remoteSelf = scenario.equals("timeoutOnly") || scenario.equals("numericAlias");
        boolean remote = remoteSelf || unknown || scenario.equals("otherHost") || scenario.equals("otherPort")
                || scenario.equals("remoteSeparatePath");
        boolean confirmed = scenario.equals("confirmed");
        boolean different = scenario.equals("differentBytes");
        Path destination = folder;
        if (alias) {
            destination = Files.createSymbolicLink(folder.resolve("alias"), folder);
        } else if (!self) {
            destination = Files.createDirectory(folder.resolve("archive"));
        }
        Path target = destination.resolve(file.getFileName());
        byte[] input = Files.readAllBytes(file);
        if (link) {
            Files.createLink(target, file);
        } else if (!self && !alias) {
            Files.write(target, different ? new byte[]{5, 6} : input);
        }
        byte[] targetBytes = Files.readAllBytes(target);
        assertEquals(self || alias || link, Files.isSameFile(file, target));
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, destination.toUri().toString());
        successAction = "MOVE";
        controlledOwner = true;
        retryMillis = 0;
        java.util.List<String> actions = new java.util.ArrayList<>();
        try (MockedConstruction<MoveAction> ignored = mockConstruction(MoveAction.class, (move, context) ->
                when(move.execute(any(), any(), any(), any(), any(), any())).thenAnswer(call -> {
                    actions.add("move");
                    if (actions.size() == 1) {
                        if (confirmed) {
                            return new ActionResult(ActionResult.Step.DELETE_SOURCE, false, null, true, null);
                        }
                        throw new IllegalStateException("The initial move returned no result.");
                    }
                    return null;
                }))) {
            start(0, 1000);
            VfsSession session = generation.pollSession;
            String uri = file.toUri().toString();
            FileObject from = remote ? observedSftpEntry(scenario, true)
                    : spy(session.resolve(uri, session.options(VFSConstants.TRANSPORT_FILE_FILE_URI)));
            doAnswer(attempt -> {
                actions.add("cleanup");
                return true;
            }).when(from).delete();
            if (remote) {
                String role = VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS;
                FileObject to = observedSftpEntry(scenario, false);
                FileObject payload = session.resolve(target.toUri().toString(), session.options(role));
                LockIo targetReads = spy(session.lockIo(role));
                doAnswer(lookup -> {
                    assertEquals(to.getName().getURI(), lookup.getArgument(0));
                    return payload;
                }).when(targetReads).resolve(anyString());
                doReturn(targetReads).when(session).lockIo(role);
                doAnswer(call -> uri.equals(call.getArgument(0)) ? from : to)
                        .when(session).resolve(anyString(), any());
            } else {
                doReturn(from).when(session).resolve(eq(uri), any());
            }
            when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
            assertEquals(FileTask.Outcome.STATE_WRITTEN, admitted(file, "").processOnPoll(null, false));
            assertEquals(java.util.Arrays.asList("move"), actions);
            clearInvocations(session);
            FileTask.Outcome retried = admitted(file, "").processOnPoll(null, false);
            assertArrayEquals(input, Files.readAllBytes(file));
            assertArrayEquals(targetBytes, Files.readAllBytes(target));
            assertFalse(Files.exists(lock));
            verify(inject, times(1)).invoke(any(), any(), anyMap(), any());
            if (remote && !unknown && !remoteSelf) {
                verify(session, times(1)).resolve(eq(uri), any());
            }
            assertEquals(unknown ? FileTask.Outcome.STATE_WRITTEN : FileTask.Outcome.RETRIED, retried);
            if (unknown) {
                assertEquals(java.util.Arrays.asList("move"), actions);
                Path marker = folder.resolve("message.txt.fail");
                FailState state = FailState.parse(Files.readAllBytes(marker), uri, "message.txt");
                assertNotNull(state);
                assertEquals(FailState.Outcome.SUCCESS, state.outcome);
                assertFalse(state.action.clear);
                assertEquals("message.txt", state.failedTarget);
                assertEquals(1, state.attempts);
            } else {
                String expected = self || alias || remoteSelf || different ? "move" : "cleanup";
                assertEquals(java.util.Arrays.asList("move", expected), actions);
                assertFalse(Files.exists(folder.resolve("message.txt.fail")));
            }
        }
    }

    private static FileObject observedSftpEntry(String scenario, boolean input) throws Exception {
        String host = input || !scenario.equals("otherHost") ? "files.example" : "archive.example";
        if (scenario.equals("numericAlias")) {
            host = input ? "::1" : "0:0:0:0:0:0:0:1";
        }
        int port = !input && scenario.equals("otherPort") ? 2222 : 22;
        String path = !input && scenario.equals("remoteSeparatePath") ? "/archive/message.txt" : "/in/message.txt";
        GenericURLFileName root = new GenericURLFileName("sftp", host, port, 22, "user", null, "/",
                FileType.FOLDER, null);
        GenericURLFileName name = new GenericURLFileName("sftp", host, port, 22, "user", null, path,
                FileType.FILE, null);
        FileSystemOptions options = new FileSystemOptions();
        SftpFileSystemConfigBuilder config = SftpFileSystemConfigBuilder.getInstance();
        config.setSessionTimeout(options, java.time.Duration.ofSeconds(input ? 2 : 8));
        if (!input && scenario.equals("unknown")) {
            config.setProxyHost(options, "route.example");
        }
        SftpFileSystem view = mock(SftpFileSystem.class);
        doReturn(root).when(view).getRootName();
        doReturn(options).when(view).getFileSystemOptions();
        FileObject file = scenario.equals("opaque") && !input ? mock(FileObject.class)
                : mock(SftpFileObject.class, CALLS_REAL_METHODS);
        doReturn(name).when(file).getName();
        doReturn(view).when(file).getFileSystem();
        doNothing().when(file).close();
        return file;
    }

    @Test
    void settledMoveKeepsItsOutcomeWhenFinalAccountingThrows() throws Exception {
        Path done = Files.createDirectory(folder.resolve("done"));
        destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, done.toUri().toString());
        successAction = "MOVE";
        controlledOwner = true;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
        doReturn(0L).doThrow(new IllegalStateException("Local accounting failed."))
                .when(generation.pollSession).failedSourceResets();
        assertEquals(FileTask.Outcome.PROCESSED, admitted(file, "").processOnPoll(null, false));
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(done.resolve("message.txt")));
        assertFalse(Files.exists(file));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
        verify(inject, times(1)).invoke(any(), any(), anyMap(), any());
        assertEquals(1L, health.snapshot().get("filesDone"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"DELETE", "MOVE"})
    void contentThatTheFlowDidNotReadIsClosedBeforeThePostAction(String action) throws Exception {
        Path done = folder.resolve("done");
        if (action.equals("MOVE")) {
            Files.createDirectory(done);
            destinations.setProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, done.toUri().toString());
        }
        successAction = action;
        controlledOwner = true;
        start(0, 1000);
        VfsSession session = generation.pollSession;
        String uri = file.toUri().toString();
        AbstractFileObject<?> source = spy((AbstractFileObject<?>) session.resolve(uri,
                session.options(VFSConstants.TRANSPORT_FILE_FILE_URI)));
        // As on an SMB2 share, a delete or a rename of the file fails while a read handle of it is open.
        Answer<Object> shareReadOnly = call -> {
            if (source.getContent().isOpen()) {
                FileSystemException refused = new FileSystemException("Test file is open for read.");
                refused.setFault(Fault.APPLICATION);
                throw refused;
            }
            return call.callRealMethod();
        };
        doAnswer(shareReadOnly).when(source).delete();
        doAnswer(shareReadOnly).when(source).moveTo(any(FileObject.class), any(MoveOptions.class));
        doReturn(source).when(session).resolve(eq(uri), any());
        // The flow opens the body and reads none of it.
        when(inject.invoke(any(), any(), anyMap(), any())).thenAnswer(call -> {
            ((FileInjectHandler.Content) call.getArgument(3)).open();
            return true;
        });
        assertEquals(FileTask.Outcome.PROCESSED, admitted(file, "").processOnPoll(null, false));
        assertFalse(Files.exists(file));
        assertFalse(Files.exists(lock));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
        assertEquals(action.equals("MOVE"), Files.exists(done.resolve("message.txt")));
        assertEquals(0L, health.snapshot().get("failStatesPending"));
        assertEquals(1L, health.snapshot().get("filesDone"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void aReplyTransportErrorWithNoActionWritesNoFailStateAndReleasesTheLock(boolean success) throws Exception {
        successAction = "NONE";
        start(0, 1000);
        FileSystemException lost = new FileSystemException("Test reply connection lost.");
        lost.setFault(Fault.TRANSPORT);
        Classification failure = new ErrorClassifier(health.namedLog()).classify(lost, 0, ErrorClassifier.CallType.CHANGE);
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(success);
        try (MockedConstruction<ReplySender> ignored = mockConstruction(ReplySender.class, (reply, context) -> {
            when(reply.transportFailure()).thenReturn(failure);
            when(reply.resetUsable()).thenReturn(true);
        })) {
            assertEquals(success ? FileTask.Outcome.PROCESSED : FileTask.Outcome.FAILED,
                    admitted(file, "").processOnPoll(null, false));
        }
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(file));
        assertFalse(Files.exists(lock));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
        assertEquals(0L, health.snapshot().get("failStatesPending"));
        assertEquals(success ? 1L : 0L, health.snapshot().get("filesDone"));
        assertEquals(success ? 0L : 1L, health.snapshot().get("filesFailed"));
    }

    @Test
    void noneKeepsTheMediationOutcomeAndEndsTheLockAfterALocalError() throws Exception {
        successAction = "NONE";
        controlledOwner = true;
        start(0, 1000);
        when(inject.invoke(any(), any(), anyMap(), any())).thenAnswer(call -> {
            doThrow(new IllegalStateException("Local owner lookup failed."))
                    .doCallRealMethod().when(generation.pollSession).lockIo(VFSConstants.TRANSPORT_FILE_FILE_URI);
            return true;
        });
        assertEquals(FileTask.Outcome.PROCESSED, admitted(file, "").processOnPoll(null, false));
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(file));
        assertFalse(Files.exists(lock));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
        assertEquals(1L, health.snapshot().get("filesDone"));
    }

    @Test
    void exceptionBeforeMediationLeavesTheFileForOrdinaryAdmission() throws Exception {
        start(0, 1000);
        when(inject.sequence()).thenThrow(new IllegalStateException("Local sequence lookup failed."))
                .thenReturn(sequence);
        when(inject.invoke(any(), any(), anyMap(), any())).thenReturn(true);
        assertEquals(FileTask.Outcome.SKIPPED, admitted(file, "").processOnPoll(null, false));
        verify(inject, never()).invoke(any(), any(), anyMap(), any());
        assertTrue(Files.exists(file));
        assertFalse(Files.exists(lock));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
        assertEquals(FileTask.Outcome.PROCESSED, admitted(file, "").processOnPoll(null, false));
        verify(inject, times(1)).invoke(any(), any(), anyMap(), any());
    }

    @Test
    void missingSequenceReleasesBeforeAnyFileReadOrPostAction() throws Exception {
        start(0, 1000);
        when(inject.sequence()).thenReturn(null);
        assertEquals(FileTask.Outcome.MISSING_SEQUENCE, admitted(file, "").processOnPoll(null, false));
        assertTrue(Files.exists(file));
        assertFalse(Files.exists(lock));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
        verify(inject, never()).invoke(any(), any(), anyMap(), any());
    }

    @Test
    void retirementWhileMediatingLeavesSourceAndLockAndFencesFurtherReads() throws Exception {
        start(1, 0);
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        when(inject.invoke(any(), any(), anyMap(), any())).thenAnswer(call -> {
            try (InputStream stream = ((FileInjectHandler.Content) call.getArgument(3)).open()) {
                assertEquals(21, stream.read());
                inside.countDown();
                assertTrue(done.await(10, TimeUnit.SECONDS));
                // The first read filled the buffer of the content with the whole file. The flow still gets the byte
                // in the buffer, and the next read below the buffer fails.
                assertEquals(22, stream.read());
                readFailure.set(assertThrows(java.io.IOException.class, stream::read));
            }
            return true;
        });
        CompletableFuture<FileTask.Outcome> answer = generation.workerPool.reserve().submit(admitted(file, ""));
        assertTrue(inside.await(10, TimeUnit.SECONDS));
        life.exitPoll(generation);
        life.stop("pause");
        life.resume();
        generation = life.enterPoll();
        try {
            assertEquals(2, generation.number);
            assertTrue(Files.exists(lock));
        } finally {
            done.countDown();
        }
        assertEquals(FileTask.Outcome.RETIRED, answer.get(10, TimeUnit.SECONDS));
        assertNotNull(readFailure.get());
        assertArrayEquals(new byte[]{21, 22}, Files.readAllBytes(file));
        assertTrue(Files.exists(lock));
        assertFalse(Files.exists(folder.resolve("message.txt.fail")));
    }
}

