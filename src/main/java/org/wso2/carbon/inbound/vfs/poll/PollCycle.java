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

package org.wso2.carbon.inbound.vfs.poll;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.filter.FileSelector;
import org.wso2.carbon.inbound.vfs.filter.UploadCheck;
import org.wso2.carbon.inbound.vfs.processor.MoveTargetSpec;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.provider.AtomicOps;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.error.Classification;
import org.wso2.vfs.shared.failstate.ActionResult;
import org.wso2.vfs.shared.failstate.FailState;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.lock.FolderListing;
import org.wso2.vfs.shared.lock.LockIo;
import org.wso2.vfs.shared.lock.LockOutcome;
import org.wso2.vfs.shared.lock.ReservedNames;
import org.wso2.vfs.shared.log.UriMask;
import org.wso2.vfs.shared.write.SweepResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.wso2.carbon.inbound.vfs.VFSConstants.*;
import static org.wso2.vfs.shared.error.ErrorClassifier.CallType.READ;

public final class PollCycle {
    private static final Log LOG = LogFactory.getLog(PollCycle.class);

    private final FileTask.Services services;
    private final String missingMessage;
    private final VFSConfig config;
    private final FileSelector selector;
    private final boolean recursive;
    private final String rootUri;
    private final String clock;
    private final long interval;
    private boolean parentOwed = true;
    private long backoff;
    private long failedAt;

    public PollCycle(FileTask.Services services, long interval, String sequenceName) {
        this.services = services;
        missingMessage = UriMask.mask("[inbound:" + services.endpoint + "] The injecting sequence is missing. "
                + sequenceName).replaceAll("\\R", " ");
        this.config = services.config;
        this.interval = interval;
        selector = new FileSelector(config, services.endpoint, services.log);
        recursive = Utils.supportsSubDirectoryToken(config.getFileURI());
        rootUri = recursive ? Utils.sanitizeFileUriWithSub(config.getFileURI()) : config.getFileURI();
        clock = "pollwait." + services.endpoint;
    }

    public void run(Generation generation) {
        new Poll(generation).run();
    }

    private static final class Candidate {
        final String uri;
        final String name;
        final String relative;
        final FolderListing.Metadata metadata;
        final FolderListing listing;
        final FileObject listed;

        Candidate(FileObject file, String uri, FolderListing.Metadata metadata, String relative, FolderListing listing) {
            this.uri = uri;
            name = file.getName().getBaseName();
            this.relative = relative;
            this.metadata = metadata;
            this.listing = listing;
            listed = file;
        }
    }

    private final class Poll {
        final Generation generation;
        final VfsSession session;
        final List<Candidate> candidates = new ArrayList<>();
        final List<FolderListing> listings = new ArrayList<>();
        final Set<String> present = new HashSet<>();
        final List<CompletableFuture<FileTask.Outcome>> workers = new ArrayList<>();
        // The tasks that took their file in this poll. Their moves name the target folders that the poll sweeps.
        final List<FileTask> tasks = new ArrayList<>();
        long startingResets;
        boolean listed;
        boolean missingSequence;
        int count;
        boolean usedSession;
        Classification failure;
        long callStart;

        Poll(Generation generation) {
            this.generation = generation;
            session = generation.pollSession;
        }

        void run() {
            boolean skipped = false;
            try {
                callStart = System.nanoTime();
                generation.pollStarted();
                services.log.debug("Poll started.");
                services.locks.checkHeldAges();
                missingSequence = services.inject.sequence() == null;
                if (missingSequence || (backoff > 0 && now() - failedAt < backoff)) {
                    skipped = true;
                    return;
                }
                session.beforePoll();
                usedSession = true;
                startingResets = session.resetCount();
                try (FileObject root = resolveRoot()) {
                    if (root == null || !running()) {
                        return;
                    }
                    callStart = System.nanoTime();
                    FailStateStore.PendingResult pending = services.store.runPending(io());
                    if (handleFailure(pending.failure)) {
                        return;
                    }
                    for (FailState record : pending.dropped) {
                        report(record);
                    }
                    if (!running()) {
                        return;
                    }
                    listingPoint();
                    scan(root);
                    if (!listed || !running()) {
                        return;
                    }
                    backoff = 0;
                    selector.retain(present);
                    services.reads.listed(generation, present);
                    if (TestPoints.enabled()) {
                        TestPoints.hold("poll.after-listing", services.endpoint);
                    }
                    order();
                    if (!listings.isEmpty()) {
                        services.locks.sweepOrphans(io(), listings);
                        if (!swept(services.store.sweep(io(), listings))) {
                            return;
                        }
                    }
                    for (Candidate candidate : candidates) {
                        if (!running() || session.resetCount() - startingResets >= 3 || missing() || atLimit()) {
                            break;
                        }
                        admit(candidate);
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception problem) {
                handleFailure(classify(problem));
                services.log.error(Utils.withTrace("Poll failed. " + problem.getClass().getSimpleName()
                        + (failure == null ? "" : " " + failure.getStatusText()), problem));
            } finally {
                try {
                    awaitWorkers();
                    sweepMoveTargets();
                    if (missingSequence) {
                        LOG.error(missingMessage);
                    }
                } finally {
                    if (!listed && failure != null && failure.getKind().isTransport()) {
                        failedAt = now();
                        backoff = backoff == 0 ? Math.max(1000, Math.min(interval, 300000))
                                : Math.min(300000, 2 * backoff);
                    }
                    if (usedSession) {
                        session.remoteCallEnded();
                    }
                    if (skipped) {
                        generation.pollSkipped();
                    } else {
                        generation.pollFinished(listed, failure);
                    }
                }
            }
        }

        // Sweeps each move target folder that a task of this poll used, with the poll session. The cleanup allows a
        // folder once per hour. A failure here never fails the poll or a post action.
        void sweepMoveTargets() {
            Map<String, Set<String>> folders = new LinkedHashMap<>();
            for (FileTask task : tasks) {
                Map.Entry<String, String> used = task.movedInto();
                if (used != null) {
                    folders.computeIfAbsent(used.getKey(), key -> new LinkedHashSet<>()).add(used.getValue());
                }
            }
            if (folders.isEmpty()) {
                return;
            }
            if (!usedSession || !running() || failure != null && failure.getKind().isTransport()
                    || session.resetCount() - startingResets >= 3) {
                services.log.debug("The move target folders are not swept, because the poll session is not usable.");
                return;
            }
            for (Map.Entry<String, Set<String>> entry : folders.entrySet()) {
                for (String folder : entry.getValue()) {
                    try {
                        SweepResult result = services.moves.get(entry.getKey()).sweep(session, services.cleanup, folder);
                        if (result.ended() == SweepResult.End.TRANSPORT_ERROR) {
                            return;
                        }
                    } catch (RuntimeException error) {
                        services.log.warn("The temp files of a move target folder were not swept at "
                                + UriMask.mask(folder) + ". " + error);
                        return;
                    }
                }
            }
        }

        void listingPoint() {
            if (TestPoints.enabled()) {
                TestPoints.hold("poll.before-listing", services.endpoint);
                TestPoints.Fault fault = TestPoints.fault("poll.listing", services.endpoint);
                if (fault != null && "runtime-exception".equals(fault.token())) {
                    throw new IllegalStateException("The listing test point failed.");
                }
            }
        }

        FileObject resolveRoot() throws IOException, InterruptedException {
            for (int attempt = 1; running(); attempt++) {
                callStart = System.nanoTime();
                try {
                    FileObject root = session.resolve(rootUri, session.options(TRANSPORT_FILE_FILE_URI));
                    failure = null;
                    return root;
                } catch (FileSystemException problem) {
                    handleFailure(classify(problem));
                    if (attempt >= config.getMaxRetryCount()) {
                        services.log.error("Could not resolve the poll root. " + failure.getStatusText());
                        return null;
                    }
                    services.log.warn("Failed to resolve the file URI: " + rootUri + ", in attempt " + attempt + ", "
                            + failure.getStatusText() + " Retrying in " + config.getReconnectTimeout()
                            + " milliseconds.");
                    if (!pause(config.getReconnectTimeout())) {
                        break;
                    }
                }
            }
            return null;
        }

        boolean handleFailure(Classification cause) {
            if (cause == null) {
                return false;
            }
            failure = cause;
            if (cause.getKind().isTransport()) {
                session.resetCurrentLocation(TRANSPORT_FILE_FILE_URI, rootUri, cause.getCause());
                generation.transportFault(cause);
            }
            return true;
        }

        Classification classify(Exception problem) {
            return services.classifier.classify(problem,
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - callStart), READ);
        }

        void scan(FileObject root) throws IOException {
            callStart = System.nanoTime();
            FileType type = root.getType();
            if (type == FileType.IMAGINARY && AtomicOps.existsNow(root)) {
                root.refresh();
                type = root.getType();
                if (type == FileType.IMAGINARY) {
                    throw new IOException("The root type changed during the poll.");
                }
            }
            if (type == FileType.FOLDER || type == FileType.FILE) {
                services.log.pollRootPresent(rootUri);
            }
            if (type == FileType.FOLDER) {
                list(root, "");
            } else {
                String uri = singleFileUri(root);
                FolderListing parent = parentOwed ? parentListing(root) : null;
                if (type == FileType.IMAGINARY) {
                    services.log.missingPollRoot(rootUri);
                    FailStateStore.SweepResult result = services.store.checkAbsentSingleFile(io(), uri, parent);
                    if (!swept(result)) {
                        return;
                    }
                    parentOwed = false;
                    services.locks.checkAbsentSingleFile(io(), uri);
                } else if (type == FileType.FILE) {
                    if (!reserved(root.getName().getBaseName(), false)) {
                        candidate(root, uri, metadata(root), "", parent);
                    }
                }
            }
            listed = true;
        }

        String singleFileUri(FileObject file) {
            String configured = Utils.stripVfsSchemeIfPresent(rootUri);
            String scheme = file.getName().getScheme() + ":";
            // A provider can render an escaped path delimiter as a literal query delimiter.
            // Keep the original URI for every subsequent lookup. Native paths still need the provider's URI.
            return configured.regionMatches(true, 0, scheme, 0, scheme.length())
                    ? configured : file.getName().getURI();
        }

        FolderListing parentListing(FileObject file) throws IOException {
            try {
                FileObject parent = file.getParent();
                if (parent == null || !AtomicOps.existsNow(parent)) {
                    parentOwed = false;
                    return null;
                }
                Map<String, FolderListing.Metadata> names = new LinkedHashMap<>();
                String marker = file.getName().getBaseName() + ".fail";
                for (FileObject child : parent.getChildren()) {
                    String name = child.getName().getBaseName();
                    names.put(name, marker.equals(name) ? metadata(child) : new FolderListing.Metadata(-1, 0));
                }
                return FolderListing.of(parent.getName().getURI(), names);
            } catch (FileSystemException problem) {
                if (classify(problem).getKind().isTransport()) {
                    throw problem;
                }
                services.log.warn("Could not list the parent folder of " + file.getName().getURI());
                parentOwed = false;
                return null;
            }
        }

        boolean reserved(String name, boolean folder) {
            return ReservedNames.isReserved(name, folder, config.getTempFolderName(), config.getTempFileSuffix());
        }

        FolderListing.Metadata metadata(FileObject file) throws FileSystemException {
            return new FolderListing.Metadata(file.getContent().getSize(), file.getContent().getLastModifiedTime());
        }

        // The parts of a listing entry that can be read. A part that the server did not send stays unknown: -1 for
        // the size and 0 for the time. Some SFTP servers send no time for any file. The size is read only to know
        // whether it is there.
        FolderListing.Metadata partial(FileObject file) throws FileSystemException {
            long size = -1;
            long modified = 0;
            try {
                size = file.getContent().getSize();
            } catch (FileSystemException unread) {
                if (classify(unread).getKind().isTransport()) {
                    throw unread;
                }
            }
            try {
                modified = file.getContent().getLastModifiedTime();
            } catch (FileSystemException unread) {
                if (classify(unread).getKind().isTransport()) {
                    throw unread;
                }
            }
            return new FolderListing.Metadata(size, modified);
        }

        // Why a file whose entry lacks a part is skipped, or null when it is taken. A file with no size is always
        // skipped, because the old transport failed such a file. A file with no time is taken, as the old transport
        // took it, unless a parameter needs the time.
        String needs(FolderListing.Metadata entry) {
            if (entry.size < 0) {
                return "the inbound needs the size";
            }
            String parameter = config.getMinimumAge() != null ? TRANSPORT_FILE_MINIMUM_AGE
                    : config.getMaximumAge() != null ? TRANSPORT_FILE_MAXIMUM_AGE
                    : FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP.equals(config.getFileSortParam()) ? FILE_SORT_PARAM : null;
            return entry.modified == 0 && parameter != null ? parameter + " needs the modified time" : null;
        }

        void list(FileObject folder, String relative) throws IOException {
            FileObject[] children = folder.getChildren();
            Map<String, FolderListing.Metadata> names = new LinkedHashMap<>();
            Set<String> files = new HashSet<>();
            Set<String> folders = new HashSet<>();
            boolean undecodable = false;
            for (FileObject child : children) {
                String name = child.getName().getBaseName();
                if (!undecodable && name.indexOf('\uFFFD') >= 0) {
                    // A name that the client could not decode is not found again by that name. The lock step then
                    // skips the file with no line, as a file that another node took. The file itself goes on as before.
                    undecodable = true;
                    services.log.warn("A file name in " + folder.getName().getURI() + " holds a character that could"
                            + " not be decoded as UTF-8. If such a file is never taken, the server uses another"
                            + " encoding for file names." + (SCHEME_SFTP.equals(folder.getName().getScheme())
                            ? " Set fileNameEncoding in the address, for example fileNameEncoding=ISO-8859-1." : ""));
                }
                FileType type;
                try {
                    type = child.getType();
                } catch (FileSystemException problem) {
                    Classification failure = classify(problem);
                    if (failure.getKind().isTransport()) {
                        throw problem;
                    }
                    // One name that the server refuses must not stop the other files of the folder. The name stays
                    // in the listing, so its lock and fail markers are not taken as orphans.
                    services.log.warn("Could not read the type of a listed file. The file is skipped. "
                            + child.getName().getURI());
                    services.log.debug("The type read failed for " + child.getName().getURI() + ". "
                            + failure.getStatusText());
                    names.put(name, new FolderListing.Metadata(-1, 0));
                    present.add(child.getName().getURI());
                    continue;
                }
                boolean directory = type == FileType.FOLDER;
                FolderListing.Metadata entry = new FolderListing.Metadata(-1, 0);
                if (type == FileType.FILE && (!reserved(name, false) || name.endsWith(".fail"))) {
                    try {
                        entry = metadata(child);
                        if (!reserved(name, false)) {
                            files.add(name);
                        }
                    } catch (FileSystemException problem) {
                        if (classify(problem).getKind().isTransport()) {
                            throw problem;
                        }
                        // The file is still in the listing, so it keeps its read error count.
                        present.add(child.getName().getURI());
                        services.log.debug("Could not read the metadata for " + child.getName().getURI());
                        if (!reserved(name, false)) {
                            // The old transport took a file with no time, so it is taken unless a parameter needs the
                            // time. A file with no size is skipped, as the old transport failed it. A skipped file
                            // gets a line that names it.
                            entry = partial(child);
                            String reason = needs(entry);
                            if (reason == null) {
                                files.add(name);
                            } else {
                                services.log.warn("The listing does not give all the details of a file, and "
                                        + reason + ". The file is skipped. " + child.getName().getURI());
                            }
                        }
                    }
                } else if (directory && recursive && !reserved(name, true)) {
                    folders.add(name);
                }
                // The type and the details are read. The close drops the parent that a type read can make with a copy
                // of the folder listing (FTP), so a file that waits for its turn keeps none. The take refreshes the
                // file anyway.
                child.close();
                names.put(name, entry);
            }
            FolderListing listing = FolderListing.of(folder.getName().getURI(), names);
            listings.add(listing);
            for (FileObject child : children) {
                String name = child.getName().getBaseName();
                if (files.contains(name)) {
                    candidate(child, child.getName().getURI(), names.get(name), relative, listing);
                } else if (folders.contains(name)) {
                    list(child, relative + "/" + name);
                }
            }
        }

        void candidate(FileObject file, String uri, FolderListing.Metadata data, String relative, FolderListing listing) {
            Candidate candidate = new Candidate(file, uri, data, relative, listing);
            candidates.add(candidate);
            present.add(candidate.uri);
        }

        void order() {
            String sort = config.getFileSortParam();
            Comparator<Candidate> comparator;
            if (FILE_SORT_VALUE_NAME.equals(sort)) {
                comparator = Comparator.comparing(candidate -> candidate.uri);
            } else if (FILE_SORT_VALUE_SIZE.equals(sort)) {
                comparator = Comparator.comparingLong(candidate -> candidate.metadata.size);
            } else if (FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP.equals(sort)) {
                comparator = Comparator.comparingLong(candidate -> candidate.metadata.modified);
            } else {
                return;
            }
            candidates.sort(config.isFileSortAscending() ? comparator : comparator.reversed());
        }

        boolean swept(FailStateStore.SweepResult result) {
            if (handleFailure(result.failure)) {
                return false;
            }
            for (FailState record : result.records) {
                report(record);
            }
            return true;
        }

        void report(FailState record) {
            if (record.action.step != ActionResult.Step.RENAME || record.action.clear) {
                services.log.debug("The source of a pending action is gone. " + record.identity.uri);
                return;
            }
            String parameter = record.moveOrigin == FailState.MoveOrigin.FALLBACK ? TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE
                    : record.outcome == FailState.Outcome.SUCCESS ? TRANSPORT_FILE_MOVE_AFTER_PROCESS
                    : TRANSPORT_FILE_MOVE_AFTER_FAILURE;
            MoveTargetSpec spec = services.targets.get(parameter);
            if (spec == null || record.failedTarget == null) {
                services.log.warn("Could not check the move target for " + record.identity.uri);
                return;
            }
            MoveTargetSpec.Target target = spec.recorded("", record.failedTarget);
            try {
                FailStateStore.Comparison result = services.store.compareTarget(session.lockIo(parameter),
                        record.identity, target.fileUri);
                switch (result.match) {
                    case MATCH:
                        services.log.debug("The move completed. " + target.fileUri);
                        break;
                    case MATCH_INFERRED:
                        generation.inferredMove();
                        services.log.warn("The move was inferred from the target. " + target.fileUri);
                        break;
                    case TRANSPORT_ERROR:
                        session.resetCurrentLocation(parameter, target.fileUri, result.failure.getCause());
                        generation.transportFault(result.failure);
                        services.log.warn("Could not check the move target. " + target.fileUri);
                        break;
                    default:
                        services.log.warn("The source is gone and the target does not hold the file. "
                                + record.identity.uri + " " + target.fileUri);
                }
            } catch (IOException problem) {
                Classification issue = classify(problem);
                if (issue.getKind().isTransport()) {
                    session.resetCurrentLocation(parameter, target.fileUri, issue.getCause());
                    generation.transportFault(issue);
                }
                services.log.warn("Could not check the move target. " + target.fileUri);
            }
        }

        void admit(Candidate file) throws InterruptedException {
            if (!selector.accept(file.uri, file.name, file.metadata.size, file.metadata.modified,
                    System.currentTimeMillis())) {
                return;
            }
            String marked = file.listing == null ? null : file.listing.markedBase(file.uri);
            boolean marker = marked != null;
            if (marker) {
                FolderListing.Metadata canonical = file.listing.names().get(marked + ".fail");
                boolean staged = !file.listing.stagingNames(marked).isEmpty();
                if (services.store.peek(file.uri, canonical == null ? -1 : canonical.size,
                        canonical == null ? 0 : canonical.modified, staged) == FailStateStore.Peek.SKIP) {
                    return;
                }
            }
            WorkerPool.Slot slot = null;
            FileTask task = null;
            boolean counted = false;
            try {
                if (!marker && generation.workerPool != null) {
                    slot = generation.workerPool.reserve();
                    if (slot == null || missing()) {
                        return;
                    }
                }
                if (!marker && config.getCheckSizeInterval() > 0) {
                    callStart = System.nanoTime();
                    UploadCheck.Result upload = UploadCheck.check(io(), file.uri, config.getCheckSizeInterval(),
                            this::pause);
                    if (!upload.stable) {
                        if (upload.failure != null && upload.failure.getKind().isTransport()) {
                            services.log.warn("The file was not taken in this poll. " + file.uri + " "
                                    + upload.failure.getStatusText());
                            handleFailure(upload.failure);
                        } else {
                            services.log.debug("The file is still uploading. " + file.uri);
                        }
                        return;
                    }
                }
                counted = generation.lifecycle.fileStarted(generation);
                if (!counted) {
                    return;
                }
                task = new FileTask(services, generation, file.uri, file.relative, listed);
                LockOutcome lock = task.acquire(file.listing);
                if (listings.isEmpty() && file.listing != null) {
                    parentOwed = false;
                }
                if (lock.kind != LockOutcome.Kind.ACQUIRED) {
                    return;
                }
                tasks.add(task);
                if (slot == null) {
                    counted = false;
                    // A file under a listed fail record is taken only at the next poll when the store finds no
                    // record now, so a look that missed the record can not take the file a second time.
                    boolean deferReplacement = marker;
                    FileTask.Outcome outcome = task.processOnPoll(file.listed, deferReplacement);
                    FailStateStore.Decision decision = task.decision();
                    if (marker && decision == FailStateStore.Decision.PROCESS) {
                        services.log.warn("The listed fail record was not found or no longer fits the file, so the"
                                + " file is taken at the next poll. " + file.uri);
                    }
                    task = null;
                    missingSequence |= outcome == FileTask.Outcome.MISSING_SEQUENCE;
                    if (decision == FailStateStore.Decision.RETRY || marker
                            && (deferReplacement || decision != FailStateStore.Decision.PROCESS)) {
                        return;
                    }
                } else {
                    CompletableFuture<FileTask.Outcome> future = slot.submit(task);
                    slot = null;
                    if (future == null) {
                        return;
                    }
                    counted = false;
                    task = null;
                    workers.add(future);
                }
                count++;
                if (config.getFileProcessingInterval() != null) {
                    pause(config.getFileProcessingInterval());
                }
            } catch (IOException problem) {
                Classification cause = classify(problem);
                if (cause.getKind().isTransport()) {
                    services.log.warn("The file was not taken in this poll. " + file.uri + " " + cause.getStatusText());
                    handleFailure(cause);
                } else {
                    services.log.debug("Could not check the upload of " + file.uri);
                }
            } finally {
                if (task != null) {
                    task.abandon();
                }
                if (counted) {
                    generation.lifecycle.fileEnded(generation, null);
                }
                if (slot != null) {
                    slot.cancel();
                }
            }
        }

        boolean atLimit() {
            Integer limit = config.getFileProcessingCount();
            return limit != null && count >= limit;
        }

        boolean missing() {
            workers.removeIf(worker -> {
                // A worker that failed stays in the list, and awaitWorkers reports it.
                if (!worker.isDone() || worker.isCompletedExceptionally()) {
                    return false;
                }
                missingSequence |= worker.join() == FileTask.Outcome.MISSING_SEQUENCE;
                return true;
            });
            return missingSequence;
        }

        void awaitWorkers() {
            boolean interrupted = false;
            try {
                for (CompletableFuture<FileTask.Outcome> worker : workers) {
                    worker.whenComplete((result, problem) -> {
                        synchronized (generation.lifecycle) {
                            generation.lifecycle.notifyAll();
                        }
                    });
                    synchronized (generation.lifecycle) {
                        while (!worker.isDone() && !generation.isRetired()) {
                            try {
                                generation.lifecycle.wait(1000);
                            } catch (InterruptedException stop) {
                                interrupted = true;
                            }
                        }
                    }
                    if (generation.isRetired()) {
                        return;
                    }
                    if (worker.isCompletedExceptionally()) {
                        services.log.error(Utils.withTrace("A file worker failed.",
                                worker.handle((result, problem) -> problem).join()));
                    } else {
                        missingSequence |= worker.join() == FileTask.Outcome.MISSING_SEQUENCE;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        boolean running() {
            return !generation.isRetired() && generation.lifecycle.state() == Lifecycle.State.RUNNING;
        }

        boolean pause(long millis) throws InterruptedException {
            long start = now();
            if (TestPoints.enabled()) {
                TestPoints.event("pollwait.deadline", services.endpoint, "deadline=" + (TestPoints.wallMillis(clock) + millis));
            }
            synchronized (generation.lifecycle) {
                long remaining = millis;
                while (remaining > 0 && running()) {
                    generation.lifecycle.wait(TestPoints.slice(clock, remaining));
                    remaining = millis - (now() - start);
                }
                return running();
            }
        }

        LockIo io() {
            return session.lockIo(TRANSPORT_FILE_FILE_URI);
        }
    }

    private long now() {
        return TimeUnit.NANOSECONDS.toMillis(TestPoints.nanoTime(clock));
    }
}
