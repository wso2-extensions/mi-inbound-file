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

package org.wso2.carbon.inbound.vfs.processor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystem;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileObject;
import org.wso2.org.apache.commons.vfs2.provider.Fault;
import org.wso2.org.apache.commons.vfs2.provider.MoveOptions;
import org.wso2.org.apache.commons.vfs2.provider.MovePhase;
import org.wso2.org.apache.commons.vfs2.provider.UnclearResultException;
import org.wso2.vfs.shared.error.ErrorClassifier;
import org.wso2.vfs.shared.failstate.ActionResult;
import org.wso2.vfs.shared.failstate.FailState;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.health.HealthRegistry;
import org.wso2.vfs.shared.lock.HeldLock;
import org.wso2.vfs.shared.lock.LockIo;
import org.wso2.vfs.shared.lock.ShareLock;
import org.wso2.vfs.shared.write.TempNames;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_URI;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS;

class PostActionTest {
    @TempDir Path root;
    private final String endpoint = "move" + UUID.randomUUID();
    private final byte[] payload = {1, 2, 3};
    private Path input;
    private Path archive;
    private String uri;
    private EndpointHealth health;
    private VFSConfig config;
    private VfsSession session;
    private ShareLock locks;
    private FailStateStore store;
    private MoveTargetSpec spec;
    private Generation generation;

    @BeforeEach
    void setUp() throws Exception {
        input = Files.createDirectories(root.resolve("in")).resolve("data.csv");
        archive = Files.createDirectory(root.resolve("done"));
        uri = input.toUri().toString();
        Files.write(input, payload);
        Properties settings = new Properties();
        settings.setProperty(TRANSPORT_FILE_FILE_URI, input.getParent().toUri().toString());
        settings.setProperty(TRANSPORT_FILE_MOVE_AFTER_PROCESS, archive.toUri().toString());
        config = new VFSConfig(settings);
        health = HealthRegistry.register(HealthRegistry.Kind.INBOUND, endpoint, 1000);
        locks = ShareLock.create(endpoint, true, config.getLockStaleAge(), health);
        store = FailStateStore.forEndpoint(endpoint, config.getFailStateParameters(), locks, health);
        session = spy(new VfsSession(endpoint, 1, config, 1000, health.namedLog()));
        session.enter();
        spec = MoveTargetSpec.from(TRANSPORT_FILE_MOVE_AFTER_PROCESS, config.getMoveAfterProcess(), config,
                health.namedLog());
        generation = mock(Generation.class);
    }

    @AfterEach
    void tearDown() {
        if (session != null) {
            session.leave();
            session.closeAfterAbort();
        }
        HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, endpoint);
    }

    private FileObject source() throws Exception {
        return session.resolve(uri, session.options(TRANSPORT_FILE_FILE_URI));
    }

    private FileIdentity identity(boolean digest) throws Exception {
        StringBuilder hex = new StringBuilder();
        for (byte value : MessageDigest.getInstance("SHA-256").digest(payload)) {
            hex.append(String.format("%02x", value & 255));
        }
        return new FileIdentity(uri, payload.length, Files.getLastModifiedTime(input).toMillis(),
                FileIdentity.TimeSource.COMMAND, digest ? hex.toString() : null);
    }

    @Test
    void falseDeleteRecordsTheFailedActionAndTheNextAdmissionSkipsIt() throws Exception {
        System.out.println("Marker output library " + org.apache.commons.io.file.PathUtils.class
                .getProtectionDomain().getCodeSource().getLocation());
        FileObject refused = mock(FileObject.class);
        when(refused.delete()).thenReturn(false);
        DeleteAction delete = new DeleteAction(new ErrorClassifier(health.namedLog()));
        LockIo io = session.lockIo(TRANSPORT_FILE_FILE_URI);
        HeldLock held = locks.acquire(io, uri, ShareLock.Mode.LISTENER, null).handle;
        assertNotNull(held);
        FileIdentity id = store.beginFile(io, held, uri).identity;
        ActionResult result = delete.execute(session, refused, uri);
        assertNotNull(result, "Refused delete evidence");
        assertEquals(ActionResult.Step.NONE, result.step, "Refused delete evidence");
        assertTrue(result.clear, "Refused delete evidence");
        assertEquals(FailStateStore.Write.WRITTEN,
                store.recordFailure(io, held, id, FailState.Outcome.SUCCESS, FailState.Action.DELETE, null, result),
                "Refused delete evidence");
        assertTrue(Files.isRegularFile(input.resolveSibling("data.csv.fail")), "Refused delete evidence");
        System.out.println("Observed refused delete, clear NONE, durable record");
        HeldLock next = locks.acquire(io, uri, ShareLock.Mode.LISTENER, null).handle;
        assertNotNull(next);
        try {
            assertEquals(FailStateStore.Decision.SKIP, store.beginFile(io, next, uri).decision,
                    "Next admission evidence");
            assertArrayEquals(payload, Files.readAllBytes(input), "Next admission evidence");
            verify(refused, times(1).description("Next admission evidence")).delete();
            System.out.println("Observed next admission skipped unchanged file");
        } finally {
            next.releaseChecked(io);
        }
    }

    @Test
    void repeatedMovesUseTheSameFolderUntilTheSessionResets() throws Exception {
        MoveAction move = new MoveAction(spec, config, health.namedLog());
        MoveTargetSpec.Target first = spec.target("/a", "one.csv", 0);
        FileObject file = source();
        FileSystem fs = file.getFileSystem();
        assertNull(move.execute(session, generation, file, identity(true), first, store));
        assertArrayEquals(payload, Files.readAllBytes(archive.resolve("a/one.csv")));
        Files.write(input, payload);
        MoveTargetSpec.Target second = spec.target("/b", "two.csv", 0);
        assertNull(move.execute(session, generation, source(), identity(true), second, store));
        assertArrayEquals(payload, Files.readAllBytes(archive.resolve("b/two.csv")));
        verify(session, times(1)).rememberFolder(eq(spec.parameter), eq(first.folderUri), any());
        assertTrue(session.hasSeenFolder(spec.parameter, first.folderUri));
        assertFalse(session.hasSeenFolder("another", first.folderUri));
        session.reset(fs, null);
        assertFalse(session.hasSeenFolder(spec.parameter, first.folderUri));
        Files.write(input, payload);
        assertNull(move.execute(session, generation, source(), identity(true), first, store));
        verify(session, times(2)).rememberFolder(eq(spec.parameter), eq(first.folderUri), any());
    }

    @ParameterizedTest
    @CsvSource({"true", "false"})
    void aMoveFolderThatAnotherWorkerMadeFirstCountsAsMade(boolean madeByOther) throws Exception {
        MoveTargetSpec.Target target = spec.target("/a", "one.csv", 0);
        FileObject parent = spy(session.resolve(target.parentUri, session.options(spec.parameter)));
        // As on SFTP, the create fails when the folder is there.
        doAnswer(call -> {
            if (madeByOther) {
                Files.createDirectory(archive.resolve("a"));
            }
            FileSystemException refused = new FileSystemException("Test folder create refused.");
            refused.setFault(Fault.APPLICATION);
            throw refused;
        }).when(parent).createFolder();
        doReturn(parent).when(session).resolve(eq(target.parentUri), any());
        ActionResult result = new MoveAction(spec, config, health.namedLog()).execute(session, generation, source(),
                identity(true), target, store);
        verify(parent, times(1)).createFolder();
        if (madeByOther) {
            assertNull(result);
            assertArrayEquals(payload, Files.readAllBytes(archive.resolve("a/one.csv")));
            assertFalse(Files.exists(input));
        } else {
            assertNotNull(result);
            assertEquals(ActionResult.Step.NONE, result.step);
            assertTrue(result.clear);
            assertArrayEquals(payload, Files.readAllBytes(input));
        }
    }

    @Test
    void oneSessionCannotPopulateAnotherSessionsFolderObservation() throws Exception {
        VfsSession other = new VfsSession(endpoint, 2, config, 1000, health.namedLog());
        other.enter();
        try {
            String folder = spec.target("", "one.csv", 0).folderUri;
            session.rememberFolder(spec.parameter, folder, source().getFileSystem());
            assertFalse(other.hasSeenFolder(spec.parameter, folder));
            FileSystem otherRoot = mock(FileSystem.class);
            session.rememberFolder("independent", "file:///other", otherRoot);
            session.reset(source().getFileSystem(), null);
            assertTrue(session.hasSeenFolder("independent", "file:///other"));
            assertFalse(session.hasSeenFolder(spec.parameter, folder));
        } finally {
            other.leave();
            other.closeAfterAbort();
        }
    }

    @ParameterizedTest
    @CsvSource({"COPY,true", "DELETE_SOURCE,true", "COMMIT,false"})
    void resetsOnlyTheRootsThatCanHaveLostTheConnection(MovePhase phase, boolean resetsSource) throws Exception {
        FileSystem sourceRoot = mock(FileSystem.class);
        FileSystem targetRoot = source().getFileSystem();
        doReturn(true).when(session).reset(eq(sourceRoot), any());
        AbstractFileObject file = mock(AbstractFileObject.class);
        when(file.getFileSystem()).thenReturn(sourceRoot);
        doThrow(new UnclearResultException("Unknown move result.", new java.io.EOFException(), phase,
                "/done/attempt", false)).when(file).moveTo(any(FileObject.class), any(MoveOptions.class));
        ActionResult result = new MoveAction(spec, config, health.namedLog()).execute(session, generation, file,
                identity(true), spec.target("", "absent.csv", 0), store);
        assertNotNull(result);
        assertEquals(phase == MovePhase.DELETE_SOURCE, result.committed);
        verify(session, times(resetsSource ? 1 : 0)).reset(eq(sourceRoot), any());
        verify(session, times(1)).reset(eq(targetRoot), any());
        verify(file, times(1)).moveTo(any(FileObject.class), any(MoveOptions.class));
    }

    @ParameterizedTest
    @CsvSource({"true,true", "false,false"})
    void uncertainDeleteOnlyLooksOnceAndNeverDeletesTheReplacement(boolean leaveFile, boolean fails) throws Exception {
        FileObject initial = source();
        FileObject fault = mock(FileObject.class);
        when(fault.getFileSystem()).thenReturn(initial.getFileSystem());
        when(fault.delete()).thenAnswer(call -> {
            if (leaveFile) {
                Files.write(input, new byte[]{6});
            } else {
                Files.delete(input);
            }
            throw new FileSystemException(new java.io.EOFException());
        });
        ActionResult result = new DeleteAction(new ErrorClassifier(health.namedLog())).execute(session, fault, uri);
        assertEquals(fails, result != null);
        if (fails) {
            assertFalse(result.clear);
            assertEquals(ActionResult.Step.NONE, result.step);
            assertArrayEquals(new byte[]{6}, Files.readAllBytes(input));
        }
        verify(fault, times(1)).delete();
        verify(session, times(1)).reset(eq(initial.getFileSystem()), any());
    }

    @Test
    void aSlowUnclearRenameResetsOnceAndLeavesTheLookForTheNextPoll() throws Exception {
        AbstractFileObject file = mock(AbstractFileObject.class);
        FileSystem root = source().getFileSystem();
        when(file.getFileSystem()).thenReturn(root);
        doAnswer(call -> {
            Thread.sleep(5100);
            throw new UnclearResultException("Delayed move response.", new java.io.EOFException(),
                    MovePhase.RENAME, null, false);
        }).when(file).moveTo(any(FileObject.class), any(MoveOptions.class));
        clearInvocations(session);
        ActionResult result = new MoveAction(spec, config, health.namedLog()).execute(session, generation, file,
                identity(false), spec.target("", "absent.csv", 0), store);
        assertNotNull(result);
        assertFalse(result.failure.isFast());
        assertFalse(result.clear);
        verify(session, times(1)).reset(eq(root), any());
        verify(session, never()).resolve(eq(uri), any());
        assertTrue(Files.exists(input));
    }

    @ParameterizedTest
    @CsvSource({
        "RENAME, true,  false, false, true,  false, true, false",
        "RENAME, false, false, false, true,  false, true, true",
        "RENAME, true,  false, false, false, false, false, false",
        "RENAME, true,  true,  false, true,  true,  false, false",
        "COPY, true,    true,  true,  false, true,  false, false",
        "COMMIT, true,  true,  false, true,  false, true, false",
        "COMMIT, false, true,  false, true,  false, true, false",
        "COMMIT, false, true,  false, false, true,  false, false",
        "COMMIT, true,  true,  false, false, true,  false, false",
        "COMMIT, true,  true,  true,  true,  true,  false, false",
        "AFTER_DELETE, true, true, false, true, false, true, false",
        "DELETE_SOURCE, true, false, false, true, false, true, false",
        "DELETE_SOURCE, true, true, false, true, true, false, false"
    })
    void settlesOnlyTheAllowedServerState(MovePhase phase, boolean digest, boolean sourceLeft,
            boolean tempLeft, boolean matching, boolean expectSource, boolean done, boolean inferred) throws Exception {
        FileIdentity id = identity(digest);
        FileObject actual = source();
        AbstractFileObject file = mock(AbstractFileObject.class);
        when(file.getFileSystem()).thenReturn(actual.getFileSystem());
        MoveTargetSpec.Target target = spec.target("", "archive.csv", 0);
        Path finalFile = archive.resolve("archive.csv");
        String[] attempt = {null};
        doAnswer(call -> {
            MoveOptions options = call.getArgument(1);
            attempt[0] = options.getTempFile().getName().getBaseName();
            assertTrue(TempNames.inUse(attempt[0]));
            if (!sourceLeft) {
                Files.delete(input);
            }
            Files.write(finalFile, matching ? payload : new byte[]{3, 2, 1});
            Files.setLastModifiedTime(finalFile, FileTime.fromMillis(id.mtime));
            if (tempLeft) {
                Files.write(archive.resolve(attempt[0]), payload);
            }
            throw new UnclearResultException("Unknown move result.", new java.io.EOFException(), phase,
                    options.getTempFile().getName().getPath(), false);
        }).when(file).moveTo(any(FileObject.class), any(MoveOptions.class));
        ActionResult result = new MoveAction(spec, config, health.namedLog()).execute(
                session, generation, file, id, target, store);
        assertEquals(done, result == null);
        assertEquals(expectSource, Files.exists(input));
        assertArrayEquals(matching ? payload : new byte[]{3, 2, 1}, Files.readAllBytes(finalFile));
        assertFalse(TempNames.inUse(attempt[0]));
        verify(generation, times(inferred ? 1 : 0)).inferredMove();
        verify(file, times(1)).moveTo(any(FileObject.class), any(MoveOptions.class));
        if (result != null) {
            assertEquals(phase.name(), result.step.name());
            assertFalse(result.clear);
            assertEquals(phase == MovePhase.DELETE_SOURCE, result.committed);
            assertTrue(result.temp.endsWith(attempt[0]));
        }
    }
}
