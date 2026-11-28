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

import org.wso2.org.apache.commons.vfs2.NameScope;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileObject;
import org.wso2.org.apache.commons.vfs2.provider.CommitResult;
import org.wso2.org.apache.commons.vfs2.provider.CommitStep;
import org.wso2.org.apache.commons.vfs2.provider.UnclearResultException;
import org.wso2.vfs.shared.write.TempName;
import org.wso2.vfs.shared.write.TempNames;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.wso2.carbon.inbound.vfs.FileInjectHandler;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.carbon.inbound.vfs.VFSConstants;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.processor.DeleteAction;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.carbon.inbound.vfs.processor.MoveTargetSpec;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.provider.AtomicOps;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.error.Classification;
import org.wso2.vfs.shared.error.ErrorClassifier;
import org.wso2.vfs.shared.error.ErrorKind;
import org.wso2.vfs.shared.failstate.ActionResult;
import org.wso2.vfs.shared.failstate.FailState;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.lock.FolderListing;
import org.wso2.vfs.shared.lock.HeldLock;
import org.wso2.vfs.shared.lock.LockIo;
import org.wso2.vfs.shared.log.UriMask;
import org.wso2.vfs.shared.write.TempCleanup;
import org.wso2.vfs.shared.lock.LockOutcome;
import org.wso2.vfs.shared.lock.ShareLock;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.write.AtomicWriter;
import org.wso2.carbon.inbound.vfs.reply.ReplySender;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_URI;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILURE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS;
import static org.wso2.vfs.shared.error.ErrorClassifier.CallType.READ;

public final class FileTask {
    public enum Outcome { PROCESSED, FAILED, SKIPPED, MISSING_SEQUENCE, RETRIED, STATE_WRITTEN, STATE_PENDING,
        STATE_NOT_NEEDED, RETIRED }

    public static final class Services {
        final String endpoint;
        final VFSConfig config;
        final ShareLock locks;
        final FailStateStore store;
        final ReadErrorCounter reads;
        final FileInjectHandler inject;
        final ErrorClassifier classifier;
        final DeleteAction delete;
        final NamedLog log;
        final AtomicWriter writer;
        final Map<String, MoveTargetSpec> targets;
        final Map<String, MoveAction> moves = new HashMap<>();
        // Sweeps the temp files that a cut copy of a move left in a target folder, at most once per folder per hour.
        final TempCleanup cleanup;

        public Services(String endpoint, VFSConfig config, ShareLock locks, FailStateStore store,
                        ReadErrorCounter reads, FileInjectHandler inject, Map<String, MoveTargetSpec> targets,
                        EndpointHealth health) {
            this.endpoint = endpoint;
            this.config = config;
            this.locks = locks;
            this.store = store;
            this.reads = reads;
            this.inject = inject;
            this.targets = new HashMap<>(targets);
            this.log = health.namedLog();
            writer = new AtomicWriter(endpoint, health, locks, true, config.getTempFolderName(), config.getTempFileSuffix());
            classifier = new ErrorClassifier(log);
            delete = new DeleteAction(classifier);
            targets.forEach((key, spec) -> moves.put(key, new MoveAction(spec, config, log)));
            cleanup = new TempCleanup(endpoint, health, locks, config.getTempFileSuffix());
        }
    }

    private final Services services;
    private final Generation generation;
    private final String uri;
    private final String relative;
    private final boolean listingSucceeded;
    private PostAction postAction;
    // The parameter and the target folder of the move that this task ran, for the sweep at the end of the poll.
    private volatile Map.Entry<String, String> movedInto;
    private HeldLock held;
    private FileObject source;
    private FileInjectHandler.Content content;
    private VfsSession session;
    private FileIdentity identity;
    private FailStateStore.Decision decision;
    private boolean resetUsable = true;

    public FileTask(Services services, Generation generation, String uri, String relative, boolean listingSucceeded) {
        this.services = services;
        this.generation = generation;
        this.uri = uri;
        this.relative = relative;
        this.listingSucceeded = listingSucceeded;
    }

    /** Retirement before the first lock call cancels the admission without taking a handle. */
    public LockOutcome acquire(FolderListing listing) {
        point(1);
        if (generation.isRetired()) {
            throw new java.util.concurrent.CancellationException("The file generation has retired.");
        }
        session = generation.pollSession;
        LockOutcome lock = services.config.isFileLocking()
                ? services.locks.acquire(io(), uri, ShareLock.Mode.LISTENER, listing)
                : services.locks.enterLocalOnly(uri, 1, 0);
        held = lock.handle;
        if (lock.connectionLost() || lock.kind == LockOutcome.Kind.TRANSPORT_ERROR) {
            reset(lock.classification);
        }
        return lock;
    }

    private LockIo io() {
        return session.lockIo(TRANSPORT_FILE_FILE_URI);
    }

    private void point(int step) {
        if (TestPoints.enabled()) {
            String point = "filetask.step." + step;
            if (step == 12) {
                TestPoints.event(point, LockIo.pointKey(uri), "endpoint=" + services.endpoint,
                        "generation=" + generation.number);
            } else {
                hold(point);
            }
        }
    }

    private void hold(String point) {
        if (TestPoints.enabled()) {
            TestPoints.hold(point, LockIo.pointKey(uri), "endpoint=" + services.endpoint,
                    "generation=" + generation.number);
        }
    }

    public void abandon() {
        if (held != null) {
            held.abandon();
            held = null;
        }
    }

    private void reset(Classification failure) {
        resetUsable = session.resetCurrentLocation(TRANSPORT_FILE_FILE_URI, uri,
                failure == null ? null : failure.getCause());
        if (failure != null) {
            generation.transportFault(failure);
        }
    }

    private void tryReset(Classification failure) {
        try {
            reset(failure);
        } catch (RuntimeException unavailable) {
            resetUsable = false;
            services.log.warn("The source session could not be reset.");
        }
    }

    private static boolean transport(Classification failure) {
        return failure != null && failure.getKind().isTransport();
    }

    private void releaseChecked() {
        if (held != null && !generation.isRetired()) {
            LockIo lock = io();
            HeldLock handle = held;
            held = null;
            LockOutcome end = handle.releaseChecked(lock);
            if (transport(end.classification)) {
                reset(end.classification);
            }
        }
    }

    private void releaseAfterAction() {
        if (generation.isRetired()) {
            return;
        }
        LockIo lock = io();
        HeldLock handle = held;
        held = null;
        boolean[] withheld = {false};
        LockOutcome end = handle.releaseAfterAction(lock, () -> {
            reset(null);
            withheld[0] = !resetUsable || generation.isRetired()
                    || generation.lifecycle.state() != Lifecycle.State.RUNNING;
            return withheld[0] ? null : io();
        });
        if (!withheld[0] && transport(end.classification)) {
            reset(end.classification);
        }
    }

    FailStateStore.Decision decision() {
        return decision;
    }

    public Outcome processOnPoll(FileObject listed, boolean retryOnly) {
        try {
            return process(generation.pollSession, listed, retryOnly);
        } finally {
            generation.lifecycle.fileEnded(generation, null);
        }
    }

    Outcome process(VfsSession owner, FileObject listed, boolean retryOnly) {
        if (held == null) {
            throw new IllegalStateException("The file task requires a held lock.");
        }
        session = owner;
        try {
            point(2);
            if (owner != generation.pollSession) {
                hold("worker.after-acquire");
            } else if (retryOnly) {
                hold("retry.after-acquire");
            }
            if (generation.isRetired()) {
                return Outcome.RETIRED;
            }
            point(3);
            if (generation.isRetired()) {
                return Outcome.RETIRED;
            }
            FailStateStore.BeginResult begin = services.store.beginFile(io(), held, uri);
            if (begin.handleTransferred) {
                held = null;
            }
            decision = begin.decision;
            identity = begin.identity;
            switch (begin.decision) {
                case SKIP:
                case FILE_GONE:
                    releaseChecked();
                    return Outcome.SKIPPED;
                case TRANSPORT_ERROR:
                    return readTransport(begin.failure, false);
                case RETRY:
                    return retry(begin.state);
                default:
                    if (retryOnly) {
                        releaseChecked();
                        return Outcome.SKIPPED;
                    }
                    return mediate(listed);
            }
        } catch (RuntimeException failure) {
            if (postAction != null) {
                return postAction.interrupted(failure);
            }
            services.log.warn("File processing stopped. " + uri + " "
                    + services.classifier.classify(failure, 0, READ).getStatusText());
            releaseChecked();
            return Outcome.SKIPPED;
        } finally {
            try {
                point(13);
                closeContent();
                if (source != null) {
                    try {
                        source.close();
                    } catch (FileSystemException | RuntimeException failure) {
                        services.log.debug("Could not close the source file.");
                    }
                }
            } finally {
                abandon();
                // The poll keeps its tasks until it ends, so the task lets go of its closed content and file here.
                // A closed SFTP stream still refers to its channel and the channel's buffers.
                content = null;
                source = null;
                point(14);
            }
        }
    }

    private Outcome mediate(FileObject listed) {
        point(4);
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        long start = System.nanoTime();
        try {
            if (listed != null && session == generation.pollSession && session.containsListedObject(listed)) {
                source = listed;
                source.refresh();
            } else {
                source = session.resolve(uri, session.options(TRANSPORT_FILE_FILE_URI));
            }
        } catch (FileSystemException failure) {
            return readError(failure, elapsed(start));
        }
        if (services.reads.capped(generation, identity)) {
            services.reads.flagCapped(generation, identity);
            return finish(false, services.classifier.classify(new IOException("The file read limit was reached."), 0, READ));
        }
        point(5);
        SequenceMediator seq = services.inject.sequence();
        if (seq == null) {
            releaseChecked();
            return Outcome.MISSING_SEQUENCE;
        }
        point(6);
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        ReplySender reply = new ReplySender(services.config, session, generation, services.writer, services.locks,
                services.log);
        content = new FileInjectHandler.Content(source, identity, generation, session, services.endpoint, reply,
                services.log);
        Map<String, Object> headers = headers();
        String name = (String) headers.get(VFSConstants.FILE_NAME);
        boolean success = services.inject.invoke(seq, services.endpoint, headers, content);
        postAction = new PostAction(success, name, success ? null :
                services.classifier.classify(new IOException("File mediation failed."), 0, READ));
        point(7);
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        if (content.readFailure() != null) {
            postAction = null;
            return readError(content.readFailure(), content.failureMillis());
        }
        services.reads.goodRead(generation, identity);
        if (reply.transportFailure() != null) {
            resetUsable &= reply.resetUsable();
            source = null;
            if (postAction.kind == FailState.Action.NONE) {
                // No action is owed, so no fail state is written.
                postAction.account();
                if (resetUsable && reply.transportFailure().isFast()
                        && generation.lifecycle.state() == Lifecycle.State.RUNNING) {
                    releaseChecked();
                }
                return postAction.outcome();
            }
            postAction.result = ActionResult.beforeAction(reply.transportFailure());
            postAction.account();
            return postAction.record(writeIo(postAction.result));
        }
        return finish(postAction);
    }

    private Map<String, Object> headers() {
        Map<String, Object> headers = new HashMap<>();
        headers.put(VFSConstants.FILE_NAME, source.getName().getBaseName());
        headers.put(VFSConstants.FILE_PATH, source.getName().getPath());
        // The addresses hold the password, as in the transport, unless the endpoint asks to mask it.
        boolean mask = services.config.isMaskHeaderPassword();
        String fileUri = source.getName().getURI();
        headers.put(VFSConstants.FILE_URI, mask ? UriMask.mask(fileUri) : fileUri);
        headers.put(org.apache.synapse.commons.vfs.VFSConstants.FILE_LENGTH, identity.size);
        headers.put(org.apache.synapse.commons.vfs.VFSConstants.LAST_MODIFIED, identity.mtime);
        headers.put("RELATIVE_PATH", relative);
        headers.put(VFSConstants.APPEND, String.valueOf(services.config.isAppend()));
        if (services.config.getReplyFileURI() != null) {
            String reply = services.config.getReplyFileURI();
            headers.put(VFSConstants.REPLY_FILE_URI, mask ? UriMask.mask(reply) : reply);
        }
        if (services.config.getReplayFileName() != null) {
            headers.put(VFSConstants.REPLY_FILE_NAME, services.config.getReplayFileName());
        }
        return headers;
    }

    private Outcome readTransport(Classification failure, boolean count) {
        reset(failure);
        if (count) {
            services.reads.transportFailure(generation, identity, listingSucceeded);
        }
        if (resetUsable && failure.isFast() && generation.lifecycle.state() == Lifecycle.State.RUNNING) {
            releaseChecked();
        }
        return Outcome.SKIPPED;
    }

    private Outcome readError(IOException error, long millis) {
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        Classification failure = services.classifier.classify(error, millis, READ);
        if (transport(failure)) {
            return readTransport(failure, true);
        }
        if (failure.getKind() == ErrorKind.BUSY) {
            releaseChecked();
            return Outcome.SKIPPED;
        }
        long start = System.nanoTime();
        try (FileObject fresh = session.resolve(uri, session.options(TRANSPORT_FILE_FILE_URI))) {
            if (!AtomicOps.existsNow(fresh)) {
                services.log.warn("The source file is gone. " + LockIo.pointKey(uri));
                releaseChecked();
                return Outcome.SKIPPED;
            }
        } catch (FileSystemException look) {
            Classification observation = services.classifier.classify(look, elapsed(start), READ);
            if (transport(observation)) {
                return readTransport(observation, true);
            }
        }
        return finish(false, failure);
    }

    private Outcome finish(boolean success, Classification failure) {
        String name = source == null ? uri.substring(uri.lastIndexOf('/') + 1) : source.getName().getBaseName();
        postAction = new PostAction(success, name, failure);
        return finish(postAction);
    }

    // A second close is a no-op: the streams and the data source end once.
    private void closeContent() {
        if (content != null) {
            try {
                content.close();
            } catch (RuntimeException failure) {
                services.log.warn("Could not close the file content.");
            }
        }
    }

    private Outcome finish(PostAction post) {
        // An open read handle blocks a delete or a rename on some servers, so the content ends before the owner check.
        closeContent();
        point(8);
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        point(9);
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        LockOutcome owner = held.verifyOwner(io());
        if (owner.kind == LockOutcome.Kind.TAKEN_OVER) {
            held = null;
            return Outcome.SKIPPED;
        }
        if (owner.kind == LockOutcome.Kind.OURS) {
            point(10);
            if (generation.isRetired()) {
                return Outcome.RETIRED;
            }
            long failedResets = observedSourceResets();
            post.started = true;
            try {
                post.result = perform(post.kind, post.parameter, post.target);
                post.settled = post.result == null;
            } finally {
                resetUsable &= observedSourceResets() == failedResets;
            }
        } else {
            if (post.kind == FailState.Action.NONE) {
                services.log.warn("The file lock owner could not be verified. " + LockIo.pointKey(uri));
                post.account();
                boolean usable = true;
                if (transport(owner.classification)) {
                    reset(owner.classification);
                    usable = resetUsable;
                }
                if (usable && generation.lifecycle.state() == Lifecycle.State.RUNNING
                        && (!transport(owner.classification) || owner.classification.isFast())) {
                    releaseChecked();
                }
                return post.outcome();
            }
            post.result = ActionResult.beforeAction(owner.classification);
            if (transport(post.result.failure)) {
                reset(post.result.failure);
            }
        }
        post.account();
        if (post.result != null) {
            point(11);
            if (generation.isRetired()) {
                return Outcome.RETIRED;
            }
            return post.record(writeIo(post.result));
        }
        point(12);
        releaseAfterAction();
        return post.outcome();
    }

    private long observedSourceResets() {
        try {
            return session.failedSourceResets();
        } catch (RuntimeException unavailable) {
            resetUsable = false;
            throw unavailable;
        }
    }

    private final class PostAction {
        final boolean success;
        final Classification mediationFailure;
        final FailState.Action kind;
        final String parameter;
        final MoveTargetSpec.Target target;
        ActionResult result;
        boolean started;
        boolean settled;
        boolean accounted;

        PostAction(boolean success, String name, Classification failure) {
            this.success = success;
            mediationFailure = failure;
            int action = success ? services.config.getActionAfterProcess() : services.config.getActionAfterFailure();
            kind = action == VFSConfig.MOVE ? FailState.Action.MOVE :
                    action == VFSConfig.DELETE ? FailState.Action.DELETE : FailState.Action.NONE;
            parameter = success ? TRANSPORT_FILE_MOVE_AFTER_PROCESS : TRANSPORT_FILE_MOVE_AFTER_FAILURE;
            target = kind == FailState.Action.MOVE
                    ? services.targets.get(parameter).target(relative, name, System.currentTimeMillis()) : null;
        }

        Outcome outcome() {
            return success ? Outcome.PROCESSED : Outcome.FAILED;
        }

        void account() {
            if (!accounted) {
                accounted = true;
                if (success) {
                    generation.fileDone();
                } else {
                    generation.fileFailed(mediationFailure);
                }
            }
        }

        Outcome record(LockIo write) {
            // The first failure of the post action for this file. The retries of the fail state log no line here.
            services.log.warn("The post action " + kind + " failed at step " + result.step + ". " + LockIo.pointKey(uri)
                    + (result.failure == null ? "" : " " + result.failure.getStatusText()));
            HeldLock handle = held;
            held = null;
            return written(services.store.recordFailureResult(write, handle, identity,
                    success ? FailState.Outcome.SUCCESS : FailState.Outcome.FAILURE,
                    kind, target == null ? null : target.recordedPath, result));
        }

        Outcome interrupted(RuntimeException error) {
            Classification failure = services.classifier.classify(error, 0, started
                    ? ErrorClassifier.CallType.CHANGE : READ);
            services.log.warn("File processing stopped. " + uri + " " + failure.getStatusText());
            if (generation.isRetired()) {
                return Outcome.RETIRED;
            }
            account();
            if (settled || kind == FailState.Action.NONE) {
                if (held != null && resetUsable && !transport(failure)
                        && generation.lifecycle.state() == Lifecycle.State.RUNNING) {
                    try {
                        releaseChecked();
                    } catch (RuntimeException unavailable) {
                        services.log.warn("The file lock could not be released.");
                    }
                }
                return outcome();
            }
            if (held == null) {
                // The store already owns the handle.
                return Outcome.SKIPPED;
            }
            if (result == null) {
                result = ActionResult.beforeAction(failure);
                if (started) {
                    ActionResult.Step step = failure.getPhase() == null ? ActionResult.Step.NONE
                            : ActionResult.Step.valueOf(failure.getPhase().name());
                    result = new ActionResult(step, failure.getPhase() != null && failure.getKind() != ErrorKind.UNCLEAR,
                            failure.getTempPath(), failure.isCommitConfirmed(), failure);
                }
            }
            LockIo write = null;
            if (!transport(failure)) {
                try {
                    write = writeIo(result);
                } catch (RuntimeException unavailable) {
                    services.log.warn("The fail state write will use the next session.");
                }
            } else {
                tryReset(failure);
            }
            return record(write);
        }
    }

    private ActionResult perform(FailState.Action kind, String parameter, MoveTargetSpec.Target target) {
        if (kind == FailState.Action.NONE) {
            return null;
        }
        if (source == null) {
            long start = System.nanoTime();
            try {
                source = session.resolve(uri, session.options(TRANSPORT_FILE_FILE_URI));
            } catch (FileSystemException error) {
                Classification failure = services.classifier.classify(error, elapsed(start), READ);
                if (transport(failure)) {
                    reset(failure);
                }
                return ActionResult.beforeAction(failure);
            }
        }
        if (kind == FailState.Action.DELETE) {
            return services.delete.execute(session, source, uri);
        }
        movedInto = new AbstractMap.SimpleImmutableEntry<>(parameter, target.parentUri);
        return services.moves.get(parameter).execute(session, generation, source, identity, target, services.store);
    }

    Map.Entry<String, String> movedInto() {
        return movedInto;
    }

    private LockIo writeIo(ActionResult action) {
        if (!resetUsable || generation.isRetired() || transport(action.failure)
                && (!action.failure.isFast() || generation.lifecycle.state() != Lifecycle.State.RUNNING)) {
            return null;
        }
        return io();
    }

    private Outcome written(FailStateStore.WriteResult result) {
        if (transport(result.failure)) {
            tryReset(result.failure);
        }
        switch (result.disposition) {
            case PENDING: return Outcome.STATE_PENDING;
            case NOT_NEEDED: return Outcome.STATE_NOT_NEEDED;
            default: return Outcome.STATE_WRITTEN;
        }
    }

    private Outcome retry(FailState state) {
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        point(9);
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        LockOutcome owner = held.verifyOwner(io());
        if (owner.kind == LockOutcome.Kind.TAKEN_OVER) {
            held = null;
            return Outcome.SKIPPED;
        }
        Recovery recovery = new Recovery(state);
        if (recovery.ambiguous() && owner.kind == LockOutcome.Kind.OURS && recovery.needsPreviousTarget()) {
            services.log.warn("The failed move destination is ambiguous. Reconcile the existing marker for "
                    + LockIo.pointKey(uri));
            releaseChecked();
            return Outcome.SKIPPED;
        }
        ActionResult failure;
        if (owner.kind == LockOutcome.Kind.OURS) {
            point(10);
            if (generation.isRetired()) {
                return Outcome.RETIRED;
            }
            long failedResets = session.failedSourceResets();
            failure = recovery.run();
            resetUsable &= session.failedSourceResets() == failedResets;
        } else {
            if (transport(owner.classification)) {
                reset(owner.classification);
            }
            failure = recovery.unchanged(owner.classification);
        }
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        if (failure == null) {
            long start = System.nanoTime();
            try {
                services.store.retrySucceeded(io(), state);
                point(12);
                releaseAfterAction();
            } catch (IOException error) {
                Classification cleanup = services.classifier.classify(error, elapsed(start), ErrorClassifier.CallType.CHANGE);
                if (transport(cleanup)) {
                    return readTransport(cleanup, false);
                }
                releaseChecked();
                return Outcome.SKIPPED;
            }
            return Outcome.RETRIED;
        }
        point(11);
        if (generation.isRetired()) {
            return Outcome.RETIRED;
        }
        LockIo write = writeIo(failure);
        HeldLock handle = held;
        held = null;
        return written(recovery.preserve
                ? services.store.retryUnchanged(write, handle, state, failure.failure)
                : services.store.retryFailureResult(write, handle, state, recovery.action, recovery.path, failure));
    }

    private final class Recovery {
        private final FailState state;
        private String parameter;
        private FailState.Action action;
        private String path;
        private String observedUri;
        private long start;
        private boolean preserve;

        Recovery(FailState state) {
            this.state = state;
            use(state.moveOrigin == FailState.MoveOrigin.FALLBACK ? FailState.Action.MOVE_AFTER_FAILED_MOVE :
                    state.failed, state.failedTarget);
        }

        boolean ambiguous() {
            return state.failed == FailState.Action.MOVE && state.moveOrigin == null && state.attempts > 0
                    && state.next == FailState.Action.MOVE_AFTER_FAILED_MOVE;
        }

        boolean unknownMove() {
            return state.failed == FailState.Action.MOVE && state.action.step == ActionResult.Step.NONE
                    && !state.action.clear;
        }

        boolean needsPreviousTarget() {
            ActionResult prior = state.action;
            return !prior.committed && (unknownMove() || prior.step == ActionResult.Step.COPY && prior.temp != null
                    || prior.step == ActionResult.Step.COMMIT || prior.step == ActionResult.Step.AFTER_DELETE);
        }

        void use(FailState.Action kind, String target) {
            action = kind;
            path = target;
            parameter = kind == FailState.Action.MOVE_AFTER_FAILED_MOVE ? TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE :
                    state.outcome == FailState.Outcome.SUCCESS ? TRANSPORT_FILE_MOVE_AFTER_PROCESS :
                            TRANSPORT_FILE_MOVE_AFTER_FAILURE;
        }

        ActionResult unchanged(Classification failure) {
            preserve = true;
            ActionResult prior = state.action;
            return new ActionResult(prior.step, prior.clear, prior.temp, prior.committed, failure);
        }

        ActionResult run() {
            try {
                if (state.action.committed) {
                    return deleteSource();
                }
                ActionResult.Step step = state.action.step;
                if (needsPreviousTarget()) {
                    MoveTargetSpec.Target failed = services.targets.get(parameter).recorded(relative, path);
                    observedUri = failed.fileUri;
                    start = System.nanoTime();
                    try (FileObject target = session.resolve(observedUri, session.options(parameter))) {
                        ActionResult result = tempStep(target, failed, step);
                        if (finishedTemp || result != null) {
                            return result;
                        }
                    }
                }
                use(state.next, state.target);
                MoveTargetSpec.Target target = action == FailState.Action.DELETE || action == FailState.Action.NONE
                        ? null : services.targets.get(parameter).recorded(relative, path);
                return perform(action, parameter, target);
            } catch (IOException | IllegalArgumentException error) {
                Classification failure = services.classifier.classify(error, elapsed(start), READ);
                if (transport(failure)) {
                    session.resetCurrentLocation(parameter, observedUri, error);
                    generation.transportFault(failure);
                }
                return unchanged(failure);
            }
        }

        private boolean finishedTemp;

        ActionResult tempStep(FileObject target, MoveTargetSpec.Target failed, ActionResult.Step step) throws IOException {
            String temp = state.action.temp;
            if (temp != null) {
                int slash = temp.lastIndexOf('/');
                String name = temp.substring(slash + 1);
                TempName parsed = TempNames.parse(name, services.config.getTempFileSuffix());
                String folder = target.getName().getParent().getPath();
                String expected = (folder.endsWith("/") ? folder : folder + "/") + name;
                if (parsed == null || !parsed.targetName().equals(target.getName().getBaseName())
                        || !expected.equals(temp)) {
                    throw new IOException("Invalid recorded temporary file.");
                }
                try (FileObject staged = target.getParent().resolveFile(name, NameScope.CHILD)) {
                    start = System.nanoTime();
                    if (AtomicOps.existsNow(staged)) {
                        if (step == ActionResult.Step.COPY || step == ActionResult.Step.COMMIT
                                && state.next == FailState.Action.MOVE_AFTER_FAILED_MOVE
                                && action != FailState.Action.MOVE_AFTER_FAILED_MOVE) {
                            if (!staged.delete() && AtomicOps.existsNow(staged)) {
                                throw new IOException("Could not remove the temporary file.");
                            }
                            return null;
                        }
                        finishedTemp = true;
                        return commit(staged, target);
                    }
                }
            }
            if (unknownMove() || step == ActionResult.Step.AFTER_DELETE
                    || step == ActionResult.Step.COMMIT && !state.action.clear) {
                FailStateStore.Comparison compared = services.store.compareTarget(session.lockIo(parameter),
                        identity, target.getName().getURI());
                if (compared.match == FailStateStore.Match.TRANSPORT_ERROR) {
                    session.reset(target.getFileSystem(), compared.failure.getCause());
                    return unchanged(compared.failure);
                }
                if (compared.match == FailStateStore.Match.MATCH) {
                    finishedTemp = true;
                    if (unknownMove()) {
                        observedUri = uri;
                        String targetParameter = parameter;
                        parameter = TRANSPORT_FILE_FILE_URI;
                        source = session.resolve(uri, session.options(parameter));
                        parameter = targetParameter;
                        observedUri = target.getName().getURI();
                        AbstractFileObject.EntryRelation relation = AbstractFileObject.compareEntries(source, target);
                        if (relation == AbstractFileObject.EntryRelation.UNKNOWN) {
                            throw new IOException("The move source and destination could not be distinguished.");
                        }
                        if (relation == AbstractFileObject.EntryRelation.SAME) {
                            return perform(action, parameter, failed);
                        }
                    }
                    return deleteSource();
                }
            }
            return null;
        }

        ActionResult commit(FileObject staged, FileObject target) throws IOException {
            String name = target.getName().getBaseName();
            TempName first = TempNames.begin(name, services.config.getTempFileSuffix());
            TempName second = TempNames.begin(name, services.config.getTempFileSuffix());
            try (FileObject parent = target.getParent();
                 FileObject left = parent.resolveFile(first.name(), NameScope.CHILD);
                 FileObject right = parent.resolveFile(second.name(), NameScope.CHILD)) {
                start = System.nanoTime();
                CommitResult committed;
                try {
                    committed = CommitStep.commit(staged, target, CommitStep.Mode.OVERWRITE,
                            new CommitStep.Options(true, first.name(), left, right));
                } catch (UnclearResultException error) {
                    Classification failure = services.classifier.classify(error, elapsed(start), ErrorClassifier.CallType.CHANGE);
                    session.reset(target.getFileSystem(), error);
                    return ActionResult.of(error, failure);
                }
                if (committed.isConnectionLost()) {
                    session.reset(target.getFileSystem(), null);
                }
                if (committed.getOutcome() != CommitResult.Outcome.COMMITTED) {
                    FileSystemException error = new FileSystemException("The temporary file could not be committed.");
                    error.setFault(committed.getAtomicResult().getFault());
                    Classification failure = services.classifier.classify(error, elapsed(start), ErrorClassifier.CallType.CHANGE);
                    return new ActionResult(committed.isOldTargetDeleted() ? ActionResult.Step.AFTER_DELETE :
                            state.action.step, state.action.clear, state.action.temp, false, failure);
                }
                return deleteSource();
            } finally {
                second.end();
                first.end();
            }
        }

        ActionResult deleteSource() {
            start = System.nanoTime();
            try {
                if (source == null) {
                    source = session.resolve(uri, session.options(TRANSPORT_FILE_FILE_URI));
                }
                if (source.delete()) {
                    return null;
                }
                throw new IOException("Could not delete the source file.");
            } catch (IOException error) {
                Classification failure = services.classifier.classify(error, elapsed(start), ErrorClassifier.CallType.CHANGE);
                if (transport(failure)) {
                    reset(failure);
                }
                return new ActionResult(ActionResult.Step.DELETE_SOURCE, !transport(failure), state.action.temp, true, failure);
            }
        }
    }

    private static long elapsed(long start) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }
}

