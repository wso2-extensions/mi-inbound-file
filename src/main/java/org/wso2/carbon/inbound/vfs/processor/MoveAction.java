/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.vfs.processor;

import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.org.apache.commons.vfs2.Capability;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystem;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.provider.AtomicOps;
import org.wso2.org.apache.commons.vfs2.provider.Fault;
import org.wso2.org.apache.commons.vfs2.provider.Faults;
import org.wso2.org.apache.commons.vfs2.provider.MoveFailedException;
import org.wso2.org.apache.commons.vfs2.provider.MoveOptions;
import org.wso2.org.apache.commons.vfs2.provider.UnclearResultException;
import org.wso2.org.apache.commons.vfs2.util.FileObjectUtils;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.error.Classification;
import org.wso2.vfs.shared.error.ErrorClassifier;
import org.wso2.vfs.shared.failstate.ActionResult;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.lock.LockIo;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.write.SweepResult;
import org.wso2.vfs.shared.write.TempCleanup;
import org.wso2.vfs.shared.write.TempName;
import org.wso2.vfs.shared.write.TempNames;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.wso2.carbon.inbound.vfs.VFSConstants.IS_MOUNTED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_URI;
import static org.wso2.carbon.inbound.vfs.processor.DeleteAction.elapsed;
import static org.wso2.vfs.shared.error.ErrorClassifier.CallType.CHANGE;
import static org.wso2.vfs.shared.error.ErrorClassifier.CallType.READ;

public final class MoveAction {
    private final MoveTargetSpec spec;
    private final boolean sourceMounted;
    private final boolean keepTime;
    private final String suffix;
    private final NamedLog log;
    private final ErrorClassifier classifier;
    private final DeleteAction delete;
    private final AtomicBoolean reportedTime = new AtomicBoolean();

    public MoveAction(MoveTargetSpec spec, VFSConfig config, NamedLog log) {
        this.spec = spec;
        this.log = log;
        sourceMounted = Boolean.parseBoolean(config.getOptions(TRANSPORT_FILE_FILE_URI).get(IS_MOUNTED));
        keepTime = config.isUpdateLastModified();
        suffix = config.getTempFileSuffix();
        classifier = new ErrorClassifier(log);
        delete = new DeleteAction(classifier);
    }

    /** Returns null when the move is settled. The caller keeps the exact target for a failed action. */
    public ActionResult execute(VfsSession session, Generation generation, FileObject source, FileIdentity identity,
                                MoveTargetSpec.Target target, FailStateStore store) {
        Attempt attempt = new Attempt(session, generation, source, identity, target, store);
        try {
            ActionResult result = attempt.run();
            if (result != null) {
                session.forgetFolder(spec.parameter);
            }
            return result;
        } finally {
            session.remoteCallEnded();
        }
    }

    /** Called by the poll owner for the destination folders used during the poll. */
    public SweepResult sweep(VfsSession session, TempCleanup cleanup, String parentUri) {
        SweepResult result = cleanup.sweep(session.lockIo(spec.parameter), parentUri);
        if (result.ended() == SweepResult.End.TRANSPORT_ERROR) {
            session.resetLocation(spec.parameter, parentUri, null, result.classification().getCause());
        }
        session.remoteCallEnded();
        return result;
    }

    // The name that a resolve of the session gives (the same parser on the same address), in the file system of the
    // destination, with no probe of the server.
    static FileObject unprobed(FileObject destination, String uri) throws FileSystemException {
        FileSystem fileSystem = destination.getFileSystem();
        return fileSystem.resolveFile(fileSystem.getFileSystemManager().resolveURI(Utils.stripVfsSchemeIfPresent(uri)));
    }

    private final class Attempt {
        private final VfsSession session;
        private final Generation generation;
        private final FileObject source;
        private final FileIdentity identity;
        private final MoveTargetSpec.Target target;
        private final FailStateStore store;
        private FileObject destination;
        private FileObject current;
        private String currentUri;
        private String currentParameter;
        private long start;

        Attempt(VfsSession session, Generation generation, FileObject source, FileIdentity identity,
                MoveTargetSpec.Target target, FailStateStore store) {
            this.session = session;
            this.generation = generation;
            this.source = source;
            this.identity = identity;
            this.target = target;
            this.store = store;
        }

        FileObject resolve(String parameter, String uri) throws FileSystemException {
            currentParameter = parameter;
            currentUri = uri;
            current = null;
            start = System.nanoTime();
            current = session.resolve(uri, session.options(parameter));
            return current;
        }

        ActionResult run() {
            try {
                prepareFolder();
                destination = resolve(spec.parameter, target.fileUri);
                destination.setUpdateLastModified(keepTime);
                destination.setIsMounted(spec.mounted);
                source.setIsMounted(sourceMounted);
                if (keepTime && log.isDebugEnabled()
                        && !destination.getFileSystem().hasCapability(Capability.SET_LAST_MODIFIED_FILE)
                        && reportedTime.compareAndSet(false, true)) {
                    log.debug("The move target keeps the time set by the server.");
                }
                return move();
            } catch (FileSystemException error) {
                Classification failure = classifier.classify(error, elapsed(start), READ);
                resetCurrent(failure);
                return new ActionResult(ActionResult.Step.NONE, true, null, false, failure);
            }
        }

        void prepareFolder() throws FileSystemException {
            if (!session.hasSeenFolder(spec.parameter, target.folderUri)) {
                FileObject folder = resolve(spec.parameter, target.folderUri);
                ensureFolder(folder, spec.createFolder);
                session.rememberFolder(spec.parameter, target.folderUri, folder.getFileSystem());
            }
            if (!target.parentUri.equals(target.folderUri)) {
                ensureFolder(resolve(spec.parameter, target.parentUri), true);
            }
        }

        void ensureFolder(FileObject folder, boolean create) throws FileSystemException {
            start = System.nanoTime();
            FileType type = folder.getType();
            if (type == FileType.IMAGINARY && create) {
                start = System.nanoTime();
                try {
                    folder.createFolder();
                } catch (FileSystemException failure) {
                    // Another worker or node can make the folder between the look and the create.
                    if (Faults.isTransport(failure)) {
                        throw failure;
                    }
                    folder.refresh();
                    if (folder.getType() != FileType.FOLDER) {
                        throw failure;
                    }
                }
            } else if (type != FileType.FOLDER) {
                FileSystemException error = new FileSystemException("The configured move folder is unavailable.");
                error.setFault(type == FileType.IMAGINARY ? Fault.MISSING : Fault.APPLICATION);
                throw error;
            }
        }

        ActionResult move() throws FileSystemException {
            String name = destination.getName().getBaseName();
            TempName temp = TempNames.begin(name, suffix);
            TempName first = TempNames.begin(name, suffix);
            TempName second = TempNames.begin(name, suffix);
            try {
                String tempUri = target.sibling(temp.name());
                // The temporary names are only names for the commit, so they are resolved with no probe of the
                // server. The destination resolve before them is the connection check of the move.
                MoveOptions options = new MoveOptions(true, unprobed(destination, tempUri), first.name(),
                        unprobed(destination, target.sibling(first.name())),
                        unprobed(destination, target.sibling(second.name())));
                start = System.nanoTime();
                try {
                    FileObjectUtils.getAbstractFileObject(source).moveTo(destination, options);
                    return null;
                } catch (MoveFailedException error) {
                    Classification failure = classifier.classify(error, elapsed(start), CHANGE);
                    if (error.isConnectionLost()) {
                        resetMove(error, ActionResult.Step.valueOf(error.getPhase().name()));
                    }
                    return ActionResult.of(error, failure);
                } catch (UnclearResultException error) {
                    Classification failure = classifier.classify(error, elapsed(start), CHANGE);
                    ActionResult result = ActionResult.of(error, failure);
                    boolean fresh = resetMove(error, result.step);
                    if (!fresh || !failure.isFast() || result.step == ActionResult.Step.COPY) {
                        return result;
                    }
                    return settle(result, tempUri);
                }
            } finally {
                second.end();
                first.end();
                temp.end();
            }
        }

        boolean resetMove(Throwable error, ActionResult.Step step) {
            FileSystem from = source.getFileSystem();
            FileSystem to = destination.getFileSystem();
            if (from == to) {
                return session.reset(from, error);
            }
            if (step == ActionResult.Step.COMMIT || step == ActionResult.Step.AFTER_DELETE) {
                return session.reset(to, error);
            }
            boolean first = session.reset(from, error);
            boolean second = session.reset(to, error);
            return first && second;
        }

        void resetCurrent(Classification failure) {
            if (failure.getKind().isTransport()) {
                session.resetLocation(currentParameter, currentUri, current, failure.getCause());
            }
        }

        boolean exists(String parameter, String uri) throws FileSystemException {
            FileObject fresh = resolve(parameter, uri);
            start = System.nanoTime();
            return AtomicOps.existsNow(fresh);
        }

        ActionResult settle(ActionResult result, String tempUri) {
            if (TestPoints.enabled()) {
                TestPoints.hold("postaction.before-look", LockIo.pointKey(identity.uri));
            }
            try {
                switch (result.step) {
                    case DELETE_SOURCE:
                        return exists(TRANSPORT_FILE_FILE_URI, identity.uri) ? result : null;
                    case RENAME:
                        if (exists(TRANSPORT_FILE_FILE_URI, identity.uri)) {
                            return result;
                        }
                        break;
                    case COMMIT:
                    case AFTER_DELETE:
                        if (exists(spec.parameter, tempUri)) {
                            return result;
                        }
                        break;
                    default:
                        return result;
                }
                current = null;
                currentParameter = spec.parameter;
                currentUri = target.fileUri;
                start = System.nanoTime();
                // A commit leaves the source in place, so with no hash in the identity the store hashes the source
                // again through its own connection. A rename comes here only when its source is gone.
                LockIo sourceIo = result.step == ActionResult.Step.RENAME ? null
                        : session.lockIo(TRANSPORT_FILE_FILE_URI);
                FailStateStore.Comparison comparison = store.compareTarget(session.lockIo(spec.parameter), sourceIo,
                        identity, target.fileUri);
                if (comparison.match == FailStateStore.Match.TRANSPORT_ERROR) {
                    resetCurrent(comparison.failure);
                    return result;
                }
                if (result.step == ActionResult.Step.RENAME) {
                    return renameResult(comparison.match, result);
                }
                if (comparison.match != FailStateStore.Match.MATCH) {
                    return result;
                }
                try {
                    FileObject fresh = resolve(TRANSPORT_FILE_FILE_URI, identity.uri);
                    return delete.delete(session, fresh, identity.uri, ActionResult.Step.DELETE_SOURCE, result.temp);
                } catch (FileSystemException error) {
                    Classification failure = classifier.classify(error, elapsed(start), READ);
                    resetCurrent(failure);
                    return new ActionResult(ActionResult.Step.DELETE_SOURCE, true, result.temp, true, failure);
                }
            } catch (IOException error) {
                resetCurrent(classifier.classify(error, elapsed(start), READ));
                return result;
            }
        }

        ActionResult renameResult(FailStateStore.Match match, ActionResult result) {
            if (match == FailStateStore.Match.MATCH) {
                return null;
            }
            if (match == FailStateStore.Match.MATCH_INFERRED) {
                log.warn("Move from " + identity.uri + " to " + target.fileUri + " was inferred from size and time.");
                generation.inferredMove();
                return null;
            }
            log.warn("Move source " + identity.uri + " is gone. Target " + target.fileUri + " comparison was " + match);
            return result;
        }
    }
}
