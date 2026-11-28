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
package org.wso2.carbon.inbound.vfs.conn;

import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystem;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.cache.NullFilesCache;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.wso2.org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.RepeatLimiter;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_URI;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS;

class VfsSessionTest {

    @TempDir Path directory;

    private VfsSession session(VfsSession.Manager manager) {
        Properties properties = new Properties();
        properties.setProperty(TRANSPORT_FILE_FILE_URI, directory.toUri().toString());
        properties.setProperty(TRANSPORT_FILE_MOVE_AFTER_PROCESS,
                "sftp://example.invalid/out?transport.vfs.AvoidPermissionCheck=false");
        return new VfsSession("session", 1, new VFSConfig(properties), 0,
                NamedLog.inbound(LogFactory.getLog(getClass()), "session", new RepeatLimiter()), manager);
    }

    @Test
    void eachSessionBuildsItsOwnOptionsAndCache() throws Exception {
        VfsSession.Manager first = new VfsSession.Manager();
        VfsSession a = session(first);
        VfsSession b = session(new VfsSession.Manager());
        try {
            a.enter();
            b.enter();
            assertTrue(first.getFilesCache() instanceof NullFilesCache);
            FileSystemOptions source = a.options(TRANSPORT_FILE_FILE_URI);
            assertNotSame(source, b.options(TRANSPORT_FILE_FILE_URI));
            FileSystemOptions target = a.options(TRANSPORT_FILE_MOVE_AFTER_PROCESS);
            assertNotSame(target, b.options(TRANSPORT_FILE_MOVE_AFTER_PROCESS));
            assertEquals("false", SftpFileSystemConfigBuilder.getInstance().getAvoidPermissionCheck(target));
            a.beforePoll();
            try (FileObject file = a.resolve(directory.toUri().toString(), source)) {
                assertTrue(file.exists());
                assertTrue(a.reset(file.getFileSystem(), null));
                try (FileObject fresh = a.resolve(directory.toUri().toString(), source)) {
                    assertNotSame(file.getFileSystem(), fresh.getFileSystem());
                }
            }
        } finally {
            a.leave();
            b.leave();
            a.closeAfterAbort();
            b.closeAfterAbort();
        }
    }

    @Test
    void failedResetDropsOnlyTheCachedRootAndTheNextResolveIsFresh() throws Exception {
        VfsSession.Manager manager = new VfsSession.Manager() {
            @Override
            public void abortAndCloseFileSystem(FileSystem root) {
                throw new IllegalStateException("reset failed before eviction");
            }
        };
        VfsSession session = session(manager);
        session.enter();
        try {
            FileSystemOptions settings = session.options(TRANSPORT_FILE_FILE_URI);
            try (FileObject file = session.resolve(directory.toUri().toString(), settings);
                 FileObject other = session.resolve("ram:///other", new FileSystemOptions())) {
                assertFalse(session.reset(file.getFileSystem(), null));
                try (FileObject fresh = session.resolve(directory.toUri().toString(), settings);
                     FileObject same = session.resolve("ram:///other", new FileSystemOptions())) {
                    assertNotSame(file.getFileSystem(), fresh.getFileSystem());
                    assertSame(other.getFileSystem(), same.getFileSystem());
                    assertTrue(fresh.exists());
                }
            }
        } finally {
            session.leave();
            session.closeAfterAbort();
        }
    }

    @Test
    void currentLocationResetsTheReplacementAndReportsAResetFailureEvenAfterEviction() throws Exception {
        java.util.List<FileSystem> invalidated = new java.util.ArrayList<>();
        java.util.concurrent.atomic.AtomicBoolean fail = new java.util.concurrent.atomic.AtomicBoolean();
        VfsSession.Manager manager = new VfsSession.Manager() {
            @Override
            public void abortAndCloseFileSystem(FileSystem root) {
                invalidated.add(root);
                if (fail.get()) {
                    throw new IllegalStateException("Test reset failure.");
                }
                super.abortAndCloseFileSystem(root);
            }
        };
        VfsSession session = session(manager);
        String uri = directory.resolve("input.txt").toUri().toString();
        try {
            session.enter();
            FileSystemOptions options = session.options(TRANSPORT_FILE_FILE_URI);
            assertTrue(session.resetCurrentLocation(TRANSPORT_FILE_FILE_URI, uri, null));
            assertTrue(invalidated.isEmpty());
            FileSystem original = session.resolve(uri, options).getFileSystem();
            long attempts = session.resetCount();
            assertTrue(session.reset(original, null));
            assertEquals(attempts + 1, session.resetCount());
            FileSystem next = session.resolve(uri, options).getFileSystem();
            assertNotSame(original, next);
            long before = session.failedSourceResets();
            assertEquals(0L, before);
            fail.set(true);
            assertFalse(session.resetCurrentLocation(TRANSPORT_FILE_FILE_URI, uri, null));
            assertEquals(java.util.Arrays.asList(original, next), invalidated);
            assertTrue(session.resetCurrentLocation(TRANSPORT_FILE_FILE_URI, uri, null));
            assertEquals(2, invalidated.size());
            assertEquals(attempts + 2, session.resetCount());
            assertEquals(before + 1, session.failedSourceResets());
            long nextAction = session.failedSourceResets();
            FileSystem targetOnly = session.resolve(uri, session.options(TRANSPORT_FILE_MOVE_AFTER_PROCESS)).getFileSystem();
            assertFalse(session.reset(targetOnly, null));
            assertEquals(nextAction, session.failedSourceResets());
            fail.set(false);
            FileSystem replacement = session.resolve(uri, options).getFileSystem();
            assertTrue(session.reset(replacement, null));
            assertEquals(nextAction, session.failedSourceResets());
            assertFalse(session.resetCurrentLocation("unknown", uri, null));
            assertFalse(session.resetCurrentLocation(TRANSPORT_FILE_FILE_URI, null, null));
            assertEquals(nextAction + 2, session.failedSourceResets());
        } finally {
            session.leave();
            session.closeAfterAbort();
        }
    }

    @Test
    void resetAbortsTheRootBeforeClosingItAndStopAbortsTheManagerFirst() {
        VfsSession.Manager manager = spy(new VfsSession.Manager());
        VfsSession session = session(manager);
        AbstractFileSystem root = mock(AbstractFileSystem.class);
        doNothing().when(manager).closeFileSystem(root);
        session.enter();
        assertTrue(session.reset(root, null));
        session.leave();
        session.closeAfterAbort();
        session.closeAfterAbort();
        InOrder order = inOrder(root, manager);
        order.verify(root).abort();
        order.verify(manager).closeFileSystem(root);
        order.verify(manager).abortAll();
        order.verify(manager).close();
        verify(manager, times(1)).close();
    }

    @Test
    @Tag("testbuild")
    void wrongThreadReportsViolationBeforeThrowing() throws Exception {
        String old = System.setProperty("vfs.testpoints.dir", directory.toString());
        VfsSession session = session(new VfsSession.Manager());
        try {
            assertTrue(TestPoints.enabled());
            session.enter();
            FutureTask<AssertionError> wrong = new FutureTask<>(() -> assertThrows(AssertionError.class,
                    () -> session.options(TRANSPORT_FILE_FILE_URI)));
            new Thread(wrong, "foreign-session-user").start();
            assertNotNull(wrong.get(3, TimeUnit.SECONDS));
            try (java.util.stream.Stream<Path> paths = java.nio.file.Files.walk(directory)) {
                Path events = paths.filter(p -> p.getFileName().toString().equals("events.log")).findFirst().get();
                String log = String.join("\n", java.nio.file.Files.readAllLines(events));
                assertTrue(log.contains("session.owner-violation"));
                assertTrue(log.contains("foreign-session-user"));
            }
        } finally {
            session.leave();
            session.closeAfterAbort();
            if (old == null) {
                System.clearProperty("vfs.testpoints.dir");
            } else {
                System.setProperty("vfs.testpoints.dir", old);
            }
        }
    }
}
