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

import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystem;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.cache.NullFilesCache;
import org.wso2.org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileProvider;
import org.wso2.org.apache.commons.vfs2.provider.FileProvider;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.lock.LockIo;
import org.wso2.vfs.shared.log.NamedLog;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.wso2.carbon.inbound.vfs.VFSConstants.REPLY_FILE_URI;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_URI;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILURE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS;

public class VfsSession {

    // A probe can cache a file system even when the first resolve returns no file.
    static class Manager extends StandardFileSystemManager {
        private final Set<AbstractFileProvider> caches = new LinkedHashSet<>();

        @Override
        public void addProvider(String[] schemes, FileProvider provider) throws FileSystemException {
            super.addProvider(schemes, provider);
            remember(provider);
        }

        @Override
        public void setDefaultProvider(FileProvider provider) throws FileSystemException {
            super.setDefaultProvider(provider);
            remember(provider);
        }

        private void dropIfCached(FileSystem root) {
            for (AbstractFileProvider provider : caches) {
                for (FileSystem cached : provider.getCachedFileSystems()) {
                    if (cached == root) {
                        // The fork drops this exact instance from its provider cache before running close.
                        provider.closeFileSystem(root);
                        return;
                    }
                }
            }
        }

        private void remember(FileProvider provider) {
            if (provider instanceof AbstractFileProvider) {
                caches.add((AbstractFileProvider) provider);
            }
        }
    }

    private final Manager manager;
    private final String endpoint;
    private final long generation;
    private final NamedLog log;
    private final Map<String, FileSystemOptions> options = new HashMap<>();
    private final Map<String, SeenFolder> folders = new HashMap<>();

    private static final class SeenFolder {
        final String uri;
        final FileSystem root;

        SeenFolder(String uri, FileSystem root) {
            this.uri = uri;
            this.root = root;
        }
    }

    private final String sourceUri;
    private long resetCount;
    private long failedSourceResets;
    private boolean resetAttempted;
    private final long idleNanos;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Thread owner;
    private boolean used;
    private long lastRemoteEnd;

    public VfsSession(String endpoint, long generation, VFSConfig config, long intervalMillis, NamedLog log) {
        this(endpoint, generation, config, intervalMillis, log, new Manager());
    }

    VfsSession(String endpoint, long generation, VFSConfig config, long intervalMillis, NamedLog log, Manager manager) {
        this.endpoint = endpoint;
        this.generation = generation;
        this.log = log;
        this.manager = manager;
        this.sourceUri = config.getFileURI();
        long interval = Math.min(Long.MAX_VALUE / 3, intervalMillis);
        idleNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(60000, 3 * interval));
        try {
            manager.setClassLoader(VfsSession.class.getClassLoader());
            manager.setConfiguration(StandardFileSystemManager.class.getResource("providers.xml"));
            manager.setFilesCache(new NullFilesCache());
            manager.init();
            for (String key : new String[] {TRANSPORT_FILE_FILE_URI, TRANSPORT_FILE_MOVE_AFTER_PROCESS,
                    TRANSPORT_FILE_MOVE_AFTER_FAILURE, TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE, REPLY_FILE_URI}) {
                Map<String, String> merged = config.getOptions(key);
                if (merged != null) {
                    options.put(key, Utils.attachFileSystemOptions(merged, manager, log));
                }
            }
        } catch (FileSystemException | RuntimeException failure) {
            closeAfterAbort();
            throw new IllegalStateException("Unable to initialize the file system manager for " + endpoint, failure);
        }
    }

    public void enter() {
        enter(Thread.currentThread());
    }

    public void enter(Thread thread) {
        if (TestPoints.enabled()) {
            if (owner != null) {
                requireOwner();
                throw new IllegalStateException("Session already in use.");
            }
            owner = thread;
        }
    }

    public void leave() {
        if (TestPoints.enabled()) {
            requireOwner();
            owner = null;
        }
    }

    private void requireOwner() {
        if (TestPoints.enabled() && owner != Thread.currentThread()) {
            Thread active = owner;
            TestPoints.event("session.owner-violation", endpoint, "endpoint=" + endpoint, "generation=" + generation,
                    "owner=" + (active == null ? "none" : active.getName()),
                    "caller=" + Thread.currentThread().getName());
            throw new AssertionError("Session called outside its owner thread.");
        }
    }

    public FileSystemOptions options(String parameter) {
        requireOwner();
        FileSystemOptions value = options.get(parameter);
        if (value == null) {
            throw new IllegalArgumentException("Unknown file system options.");
        }
        return value;
    }

    public int getOpenClientCount() {
        return manager.getOpenClientCount();
    }

    public LockIo lockIo(String parameter) {
        return new LockIo(manager, options(parameter));
    }

    public boolean hasSeenFolder(String parameter, String uri) {
        requireOwner();
        SeenFolder last = folders.get(parameter);
        return last != null && last.uri.equals(uri);
    }

    public void rememberFolder(String parameter, String uri, FileSystem root) {
        requireOwner();
        folders.put(parameter, new SeenFolder(uri, root));
    }

    public void forgetFolder(String parameter) {
        requireOwner();
        folders.remove(parameter);
    }

    public FileObject resolve(String uri, FileSystemOptions settings) throws FileSystemException {
        requireOwner();
        try {
            return manager.resolveFile(Utils.stripVfsSchemeIfPresent(uri), settings);
        } finally {
            remoteCallEnded();
        }
    }

    public void remoteCallEnded() {
        requireOwner();
        lastRemoteEnd = System.nanoTime();
        used = true;
    }

    public void beforePoll() {
        requireOwner();
        if (!used || System.nanoTime() - lastRemoteEnd > idleNanos) {
            for (AbstractFileProvider cache : manager.caches) {
                for (FileSystem fs : cache.getCachedFileSystems()) {
                    reset(fs, null);
                }
            }
        }
    }

    public boolean containsListedObject(FileObject listed) {
        requireOwner();
        if (!resetAttempted) {
            return true;
        }
        FileSystem root = listed.getFileSystem();
        for (AbstractFileProvider provider : manager.caches) {
            for (FileSystem cached : provider.getCachedFileSystems()) {
                if (cached == root) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean reset(FileSystem root, Throwable cause) {
        requireOwner();
        resetCount++;
        resetAttempted = true;
        folders.values().removeIf(folder -> folder.root == root);
        try {
            manager.abortAndCloseFileSystem(root);
            return true;
        } catch (RuntimeException failure) {
            countFailedReset(root.getFileSystemOptions(), root.getRootName() == null ? null : root.getRootName().getRootURI());
            log.debug("Could not reset the file system. " + failure.getClass().getSimpleName());
            try {
                manager.dropIfCached(root);
            } catch (RuntimeException closeFailure) {
                log.debug("Could not close the dropped file system. " + closeFailure.getClass().getSimpleName());
            }
            return false;
        }
    }

    public long resetCount() {
        requireOwner();
        return resetCount;
    }

    /** Sample on the owner thread on each side of one action. */
    public long failedSourceResets() {
        requireOwner();
        return failedSourceResets;
    }

    private void countFailedReset(FileSystemOptions failedOptions, String failedRootUri) {
        FileSystemOptions sourceOptions = options.get(TRANSPORT_FILE_FILE_URI);
        if (failedOptions != null && sourceOptions != null && failedOptions.compareTo(sourceOptions) != 0) {
            return;
        }
        if (failedRootUri != null) {
            try {
                String sourceRootUri = manager.resolveURI(Utils.stripVfsSchemeIfPresent(sourceUri)).getRootURI();
                if (!failedRootUri.equals(sourceRootUri)) {
                    return;
                }
            } catch (FileSystemException invalid) {
                // An unidentified root can share the source connection.
            }
        }
        failedSourceResets++;
    }

    public void resetLocation(String parameter, String uri, FileObject resolved, Throwable cause) {
        requireOwner();
        if (resolved != null) {
            reset(resolved.getFileSystem(), cause);
            return;
        }
        resetCurrentLocation(parameter, uri, cause);
    }

    public boolean resetCurrentLocation(String parameter, String uri, Throwable cause) {
        requireOwner();
        if (uri == null || !options.containsKey(parameter)) {
            countFailedReset(options.get(parameter), null);
            return false;
        }
        try {
            String rootUri = manager.resolveURI(Utils.stripVfsSchemeIfPresent(uri)).getRootURI();
            FileSystemOptions settings = options(parameter);
            boolean usable = true;
            for (AbstractFileProvider cache : manager.caches) {
                for (FileSystem fs : cache.getCachedFileSystems()) {
                    if (rootUri.equals(fs.getRootName().getRootURI())
                            && settings.compareTo(fs.getFileSystemOptions()) == 0 && !reset(fs, cause)) {
                        usable = false;
                    }
                }
            }
            return usable;
        } catch (FileSystemException invalid) {
            countFailedReset(options.get(parameter), null);
            log.debug("Could not identify the file system after the failed resolve.");
            return false;
        }
    }

    public void contentBarrier(FileSystem root, long bytes) {
        requireOwner();
        if (TestPoints.enabled()) {
            String count = "bytes=" + bytes;
            String key = "endpoint=" + endpoint;
            String number = "generation=" + generation;
            TestPoints.hold("session.barrier", endpoint, key, number, count);
            TestPoints.Fault fault = TestPoints.fault("session.barrier", endpoint, key, number, count);
            if (fault != null && "reset".equals(fault.token())) {
                reset(root, null);
            } else if (fault != null) {
                TestPoints.event("unknown-token", endpoint, key, number, "token=" + fault.token());
            }
        }
    }

    public void abort() {
        try {
            manager.abortAll();
        } catch (RuntimeException failure) {
            log.debug("Could not abort the file system manager. " + failure.getClass().getSimpleName());
        }
    }

    public void closeAfterAbort() {
        if (TestPoints.enabled() && owner != null) {
            requireOwner();
        }
        if (closed.compareAndSet(false, true)) {
            abort();
            try {
                manager.close();
            } catch (RuntimeException failure) {
                log.debug("Could not close the file system manager. " + failure.getClass().getSimpleName());
            }
        }
    }
}
