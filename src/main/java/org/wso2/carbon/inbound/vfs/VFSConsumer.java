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

package org.wso2.carbon.inbound.vfs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.vfs.filter.FileSelector;
import org.wso2.carbon.inbound.vfs.lock.LockManager;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.carbon.inbound.vfs.processor.PostProcessingHandler;
import org.wso2.carbon.inbound.vfs.processor.PreProcessingHandler;
import org.wso2.org.apache.commons.vfs2.FileContent;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.cache.NullFilesCache;
import org.wso2.org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.wso2.org.apache.commons.vfs2.provider.UriParser;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.wso2.carbon.inbound.vfs.Utils.maskURLPassword;
import static org.wso2.carbon.inbound.vfs.Utils.stripVfsSchemeIfPresent;

public class VFSConsumer extends GenericPollingConsumer {

    private static final Log log = LogFactory.getLog(VFSConsumer.class);
    private static final String EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e";
    // minimum age of a timestamp-only fail record before it is treated as one written by an earlier version
    private static final long LEGACY_FAIL_RECORD_MIN_AGE = 60000L;
    private final VFSConfig vfsConfig;
    private final FileSelector fileSelector;
    private final PreProcessingHandler preProcessingHandler;
    private final PostProcessingHandler postProcessingHandler;
    private final FileInjectHandler fileInjectHandler;
    private final String name;
    private FileSystemManager fsManager = null;
    private ScheduledExecutorService retryScheduler;
    // URIs of failed records (MoveAfterFailedMove) that already have a retry scheduled
    private final Set<String> pendingFailedRecordRetries = ConcurrentHashMap.newKeySet();
    private boolean fileLock = true;
    private FileSystemOptions fso;
    private boolean readSubDirectories = false;
    private boolean isClosed;
    private String replyFileURI;
    private String replyFileName;
    private boolean append = false;
    private boolean resolveHostsDynamically = false;
    private Long failedRecordNextRetryDuration = 30000L; // Default 30 seconds
    private boolean isMounted = false;

    private String fileURI;

    public VFSConsumer(Properties properties,
                       String name,
                       SynapseEnvironment synapseEnvironment,
                       long scanInterval,
                       String injectingSeq,
                       String onErrorSeq,
                       boolean coordination,
                       boolean sequential) {
        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);

        this.vfsConfig = new VFSConfig(properties);

        try {
            StandardFileSystemManager mgr = new StandardFileSystemManager();
            mgr.setClassLoader(getClass().getClassLoader());
            // we don't need to cache files in the consumer side
            mgr.setFilesCache(new NullFilesCache());
            mgr.init();
            this.fsManager = mgr;
        } catch (FileSystemException e) {
            VFSTransportErrorHandler.handleException(log, "Error initializing VFS FileSystemManager", e);
        }

        // Build FSO once per poll (protocol options; auth; SFTP opts; etc.)
        this.fso = null;

        // Initialize new features
        initializeProperties();

        // Initialize retry scheduler for failed records
        this.retryScheduler = newRetryScheduler();

        // Handlers (wire your concrete actions here)
        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment,
                vfsConfig);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(Utils.getActionAfterProcess(vfsConfig, actionAfterProcess, vfsConfig.getMoveAfterProcess(), fsManager));
        this.postProcessingHandler.setOnFailAction(Utils.getActionAfterProcess(vfsConfig, vfsConfig.getActionAfterFailure(), vfsConfig.getMoveAfterFailure(), fsManager));

        this.fileSelector = new FileSelector(vfsConfig, fsManager);
        this.name = name;

        // Resolve input URI and subdirectory setting from config (supports /* or \*)
        ResolvedFileUri inFileUri = extractFileUri(VFSConstants.TRANSPORT_FILE_FILE_URI);
        if (inFileUri == null || StringUtils.isBlank(inFileUri.resolvedUri)) {
            VFSTransportErrorHandler.handleException(log, "Invalid FileURI. Check configuration. URI: " + maskURLPassword(vfsConfig.getFileURI()));
        }
        readSubDirectories = inFileUri.supportSubDirectories;
        fileURI = stripVfsSchemeIfPresent(inFileUri.resolvedUri);

        // Resolve hosts dynamically if enabled
        if (resolveHostsDynamically) {
            fileURI = resolveHostDynamically(fileURI);
        }
        //Setup SFTP Options
        try {
            fso = Utils.attachFileSystemOptions(Utils.parseSchemeFileOptions(fileURI, properties), fsManager);
        } catch (Exception e) {
            log.warn("Unable to set the sftp Options", e);
            fso = null;
        }
    }

    public VFSConsumer(Properties properties,
                       String name,
                       SynapseEnvironment synapseEnvironment,
                       String cronExpression,
                       String injectingSeq,
                       String onErrorSeq,
                       boolean coordination,
                       boolean sequential) {
        super(properties, name, synapseEnvironment, cronExpression, injectingSeq, onErrorSeq, coordination, sequential);

        this.vfsConfig = new VFSConfig(properties);

        try {
            StandardFileSystemManager mgr = new StandardFileSystemManager();
            mgr.setClassLoader(getClass().getClassLoader());
            // we don't need to cache files in the consumer side
            mgr.setFilesCache(new NullFilesCache());
            mgr.init();
            this.fsManager = mgr;
        } catch (FileSystemException e) {
            VFSTransportErrorHandler.handleException(log, "Error initializing VFS FileSystemManager", e);
        }

        this.fso = null;

        // Initialize new features
        initializeProperties();

        // Initialize retry scheduler for failed records
        this.retryScheduler = newRetryScheduler();

        // Handlers (wire your concrete actions here)
        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment,
                vfsConfig);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(Utils.getActionAfterProcess(vfsConfig, actionAfterProcess,
                vfsConfig.getMoveAfterProcess(), fsManager));
        this.postProcessingHandler.setOnFailAction(Utils.getActionAfterProcess(vfsConfig,
                vfsConfig.getActionAfterFailure(), vfsConfig.getMoveAfterFailure(), fsManager));

        this.fileSelector = new FileSelector(vfsConfig, fsManager);
        this.name = name;
        // Resolve input URI and subdirectory setting from config (supports /* or \*)
        ResolvedFileUri inFileUri = extractFileUri(VFSConstants.TRANSPORT_FILE_FILE_URI);
        if (inFileUri == null || StringUtils.isBlank(inFileUri.resolvedUri)) {
            VFSTransportErrorHandler.handleException(log, "Invalid FileURI. Check configuration. URI: " + maskURLPassword(vfsConfig.getFileURI()));
        }
        readSubDirectories = inFileUri.supportSubDirectories;
        fileURI = stripVfsSchemeIfPresent(inFileUri.resolvedUri);

        // Resolve hosts dynamically if enabled
        if (resolveHostsDynamically) {
            fileURI = resolveHostDynamically(fileURI);
        }
        //Setup SFTP Options
        try {
            fso = Utils.attachFileSystemOptions(Utils.parseSchemeFileOptions(fileURI, properties), fsManager);
        } catch (Exception e) {
            log.warn("Unable to set the sftp Options", e);
            fso = null;
        }
    }

    /**
     * Initialize new VFS features
     */
    private void initializeProperties() {

        // Reply file configuration
        this.replyFileURI = vfsConfig.getReplyFileURI();
        this.replyFileName = vfsConfig.getReplayFileName();

        // Resolve hosts dynamically
        this.resolveHostsDynamically = vfsConfig.isResolveHostsDynamically();

        // Failed record retry duration
        this.failedRecordNextRetryDuration = vfsConfig.getFailedRecordNextRetryDuration();

        if (log.isDebugEnabled()) {
            log.debug("VFS Consumer initialized with features");
        }
    }

    @Override
    public Object poll() {
        if (isClosed) {
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Polling VFS location: " + maskURLPassword(fileURI) +
                    " (recursive=" + readSubDirectories + ")");
        }

        FileObject root = initFileCheck(fileURI);

        if (root == null) {
            log.error("Resolved FileObject is null for: " + maskURLPassword(fileURI));
            return null;
        }

        try {
            if (root.getType() == FileType.FILE) {
                // Single-file mode
                if (Utils.isFailRecord(fsManager, root, fso)) {
                    retryFailedRecord(root);
                } else if (Utils.isFailedRecordInFailedList(root, vfsConfig)) {
                    scheduleFailedRecordRetry(root);
                } else {
                    processFile(root);
                }
            } else if (root.getType() == FileType.FOLDER) {
                // Directory mode (optional recursion)
                processDirectory(root);
            } else if (root.getType() == FileType.IMAGINARY) {
                // Single-file mode and the file is absent: drop its fail record, so that it is not applied to
                // the next file with this name
                Utils.releaseOrphanFailRecordOf(fsManager, root, fso);
            } else {
                // TODO: to handle FileType FILE_OR_FOLDER;
                if (log.isDebugEnabled()) {
                    log.debug("Ignoring non-file, non-folder: " + maskURLPassword(root.toString()));
                }
            }
        } catch (FileSystemException e) {
            log.error("Error during poll for: " + maskURLPassword(fileURI), e);
        } finally {
            safeClose(root);
        }

        return null;
    }

    /* =========================
       Directory & File Handling
       ========================= */

    private void processDirectory(FileObject dir) throws FileSystemException {
        if (isClosed) {
            return;
        }
        FileObject[] children = null;
        int processCount = 0;
        try {
            children = dir.getChildren();
        } catch (FileSystemException e) {
            log.error("Unable to list children of: " + maskURLPassword(dir.toString()), e);
        }

        if (children == null || children.length == 0) {
            if (log.isDebugEnabled()) {
                log.debug("Empty directory: " + maskURLPassword(dir.toString()));
            }
            return;
        }
        // 1. sort to ensure files are processed in order
        String fileSortParam = vfsConfig.getFileSortParam();
        if (StringUtils.isNotEmpty(fileSortParam)) {
            children = Utils.sortFileObjects(children, fileSortParam, vfsConfig);
        }
        for (FileObject child : children) {
            if (isClosed) {
                return;
            }
            try {
                // Skip lock/fail markers; remove fail markers whose file is gone
                String base = child.getName().getBaseName();
                if (base.endsWith(VFSConstants.LOCK_FILE_SUFFIX)) {
                    continue;
                }
                if (base.endsWith(VFSConstants.FAIL_FILE_SUFFIX)) {
                    Utils.releaseOrphanFailRecord(fsManager, child, fso);
                    continue;
                }

                // A failed record: the file was processed but its post-processing action failed.
                // Retry only that action; never inject the file again.
                if (Utils.isFailRecord(fsManager, child, fso)) {
                    retryFailedRecord(child);
                    continue;
                }
                if (Utils.isFailedRecordInFailedList(child, vfsConfig)) {
                    scheduleFailedRecordRetry(child);
                    continue;
                }

                if (child.getType() == FileType.FOLDER) {
                    if (readSubDirectories) {
                        // Recurse only if enabled
                        processDirectory(child);
                    } else if (log.isDebugEnabled()) {
                        log.debug("Skipping subdirectory (recursive disabled): " +
                                maskURLPassword(child.toString()));
                    }
                } else if (child.getType() == FileType.FILE) {
                    processCount++;
                    if (vfsConfig.getFileProcessingInterval() != null && vfsConfig.getFileProcessingInterval() != 0) {
                        // Throttle file processing if configured
                        try {
                            processFile(child);
                            Thread.sleep(vfsConfig.getFileProcessingInterval());
                        } catch (InterruptedException ignore) {
                            if (log.isDebugEnabled()) {
                                log.debug("File processing sleep interrupted");
                            }
                            Thread.currentThread().interrupt();
                        }
                    } else if (vfsConfig.getFileProcessingCount() != null && processCount <= vfsConfig.getFileProcessingCount()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Processing file (count limit " + vfsConfig.getFileProcessingCount() +
                                    "): " + maskURLPassword(child.toString()));
                        }
                        processFile(child);
                    } else if (vfsConfig.getFileProcessingCount() != null && processCount > vfsConfig.getFileProcessingCount()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Skipping file (count limit " + vfsConfig.getFileProcessingCount() +
                                    " reached): " + maskURLPassword(child.toString()));
                        }
                        break;
                    } else {
                        processFile(child);
                    }

                } else if (log.isDebugEnabled()) {
                    log.debug("Ignoring item (not file/folder): " +
                            maskURLPassword(child.toString()));
                }
            } catch (Exception e) {
                // never block remaining files on a single failure
                log.error("Error processing child: " + maskURLPassword(child.toString()), e);
            } finally {
                safeClose(child);
            }
        }
    }

    private void processFile(FileObject file) throws FileSystemException {
        if (isClosed) {
            return;
        }
        file.setIsMounted(isMounted);
        // Delegate readiness, age, size, pattern, etc. to FileSelector
        if (!fileSelector.isValidFile(file)) {
            if (log.isDebugEnabled()) {
                log.debug("File not eligible: " + maskURLPassword(file.toString()));
            }
            return;
        }

        // Acquire lock if file locking is enabled
        fileLock = vfsConfig.isFileLocking();
        LockManager lockManager = new LockManager(fileLock, vfsConfig,
                fsManager, fso);

        if (fileLock && !lockManager.acquireLock(file)) {
            log.error("Couldn't get the lock for processing the file: " +
                    maskURLPassword(file.getName().toString()));
            return;
        }

        try {
            boolean processed = false;
            boolean mediated = false;
            boolean runPostProcess = true;
            try {
                // Pre-processing hook (e.g., acquire lock, tmp rename, etc.)
                preProcessingHandler.handle(file);

                // Build transport headers
                try (FileContent content = file.getContent()) {
                    String fileName = file.getName().getBaseName();
                    String filePath = file.getName().getPath();
                    String fileURI = file.getName().getURI();

                    Map<String, Object> headers = new HashMap<>();
                    headers.put(VFSConstants.FILE_NAME, fileName);
                    headers.put(VFSConstants.FILE_PATH, filePath);
                    headers.put(VFSConstants.FILE_URI, fileURI);

                    // Add reply file information if configured
                    if (StringUtils.isNotEmpty(replyFileURI)) {
                        headers.put(VFSConstants.REPLY_FILE_URI, replyFileURI);
                    }
                    if (StringUtils.isNotEmpty(replyFileName)) {
                        headers.put(VFSConstants.REPLY_FILE_NAME, replyFileName);
                    }

                    // Add append mode flag
                    headers.put(VFSConstants.APPEND, String.valueOf(append));

                    try {
                        headers.put(org.apache.synapse.commons.vfs.VFSConstants.FILE_LENGTH, content.getSize());
                        headers.put(org.apache.synapse.commons.vfs.VFSConstants.LAST_MODIFIED, content.getLastModifiedTime());
                    } catch (FileSystemException ignore) {
                        // length/mtime are best-effort
                    }

                    fileInjectHandler.setTransportHeaders(headers);
                    fileInjectHandler.setFileURI(fileURI);

                    processed = fileInjectHandler.invoke(file, name);
                    mediated = true;
                    if (log.isDebugEnabled()) {
                        log.debug((processed ? "File processed successfully: " : "File processing failed: ")
                                + maskURLPassword(file.toString()));
                    }
                }
            } catch (Exception e) {
                if (mediated) {
                    // the error came after the file was injected (e.g. closing the content): run the action that
                    // matches the mediation result
                    log.error("Error processing file: " + maskURLPassword(file.toString()), e);
                } else if (FileNotAvailableException.isFileNotFound(e) || !exists(file)) {
                    // As in Synapse: the file was moved by another process, so there is nothing to post-process
                    log.warn("File " + maskURLPassword(file.toString()) + " is no longer available. "
                            + "It may have been moved by another process.");
                    runPostProcess = false;
                } else {
                    // As in the Synapse VFS transport and the MI file inbound endpoint, an error while reading the
                    // file is a failed processing: the failure action runs
                    log.error("Error processing file: " + maskURLPassword(file.toString()), e);
                }
            }
            if (runPostProcess) {
                runPostProcessing(file, processed);
            }
        } finally {
            // Release lock if file locking is enabled and we shouldn't skip
            if (fileLock) {
                lockManager.releaseLock(file);
                if (log.isDebugEnabled()) {
                    log.debug("Released the lock for file: " + maskURLPassword(file.toString()));
                }
            }
        }
    }

    /**
     * Runs the action after processing: the success action if the file was processed, otherwise the failure action.
     * If the action itself fails, the file is recorded as a failed record so that only this action is retried
     * later (the file is not injected again).
     */
    private void runPostProcessing(FileObject file, boolean processed) {
        try {
            if (processed) {
                postProcessingHandler.onSuccess(file);
            } else {
                postProcessingHandler.onFail(file);
            }
        } catch (Exception e) {
            log.error("Error in the " + (processed ? "success" : "failure") + " action for file: "
                    + maskURLPassword(file.toString()) + ". The file is recorded as a failed record and "
                    + (vfsConfig.getMoveAfterMoveFailure() != null
                    ? "will be moved to the MoveAfterFailedMove location." : "the action will be retried."), e);
            String failedAction = processed ? VFSConstants.FAILED_ACTION_SUCCESS : VFSConstants.FAILED_ACTION_FAILURE;
            try {
                if (vfsConfig.getMoveAfterMoveFailure() != null) {
                    String timeStamp =
                            Utils.getSystemTime(vfsConfig.getFailedRecordTimestampFormat());
                    if (!Utils.addFailedRecord(vfsConfig, file, timeStamp)) {
                        // the failed records file could not be written: fall back to a .fail marker
                        log.warn("Could not add " + maskURLPassword(file.toString()) + " to the failed records "
                                + "file. It is recorded with a " + VFSConstants.FAIL_FILE_SUFFIX + " file instead and "
                                + "the " + (processed ? "success" : "failure") + " action will be retried.");
                        Utils.markFailRecord(fsManager, file, fso, failedAction);
                    }
                } else {
                    Utils.markFailRecord(fsManager, file, fso, failedAction);
                }
            } catch (Exception failHandlingError) {
                log.error("Error in fail handling for file: " + maskURLPassword(file.toString()), failHandlingError);
                // Mark as failed record if we couldn't handle the failure
                Utils.markFailRecord(fsManager, file, fso, failedAction);
            }
        }
    }

    /**
     * Handles a file that has a {@code .fail} marker: retries only the post-processing action that failed, at most
     * once every {@code transport.vfs.FailedRecordNextRetryDuration} ms, without injecting the file again.
     * When the action succeeds, the marker is removed. If the file was replaced by a new one since it was marked,
     * the marker is removed and the new file is processed normally in the next poll.
     */
    private void retryFailedRecord(FileObject file) {
        String content = Utils.readFailRecord(fsManager, file, fso);
        if (content == null) {
            return;
        }
        Utils.FailRecord record = Utils.FailRecord.parse(content);
        if (record == null) {
            // Not a complete marker, e.g. read while it is being rewritten (empty or cut off). Keep the file
            // skipped and look again in the next poll.
            if (log.isDebugEnabled()) {
                log.debug("Fail record of " + maskURLPassword(file.toString()) + " is incomplete. Checking again "
                        + "in the next poll.");
            }
            return;
        }
        if (record.action == null) {
            // Only a timestamp: a marker from an earlier version, unless it is a marker of this version read while
            // being written. Treat it as an earlier-version marker only when it is old enough; otherwise look again
            // in the next poll.
            long retryDuration = failedRecordNextRetryDuration == null ? 0 : failedRecordNextRetryDuration;
            if (System.currentTimeMillis() - record.markedAt < Math.max(retryDuration, LEGACY_FAIL_RECORD_MIN_AGE)) {
                return;
            }
            // The failed action is unknown. Rewrite it in the current format (keeps the file skipped, records its
            // identity, and paces the warning below).
            Utils.markFailRecord(fsManager, file, fso, VFSConstants.FAILED_ACTION_UNKNOWN);
            warnUnknownFailRecord(file);
            return;
        }
        if (!record.isSameFile(file)) {
            Utils.releaseFailRecord(fsManager, file, fso);
            log.info("File " + maskURLPassword(file.toString()) + " was replaced after it was recorded as a failed "
                    + "record. The fail record is removed and the new file will be processed.");
            return;
        }
        if (failedRecordNextRetryDuration != null
                && System.currentTimeMillis() - record.markedAt < failedRecordNextRetryDuration) {
            return;
        }
        if (VFSConstants.FAILED_ACTION_UNKNOWN.equals(record.action)) {
            Utils.markFailRecord(fsManager, file, fso, VFSConstants.FAILED_ACTION_UNKNOWN);
            warnUnknownFailRecord(file);
            return;
        }

        boolean locking = vfsConfig.isFileLocking();
        LockManager lockManager = new LockManager(locking, vfsConfig, fsManager, fso);
        if (locking && !lockManager.acquireLock(file)) {
            return;
        }
        try {
            // The marker was read before the lock was taken. Another poller may have handled this record since
            // (marker removed or rewritten, or a new file with its own marker), so act only if it is unchanged.
            if (locking && !content.equals(Utils.readFailRecord(fsManager, file, fso))) {
                if (log.isDebugEnabled()) {
                    log.debug("Fail record of " + maskURLPassword(file.toString()) + " changed while waiting for "
                            + "the lock. Skipping the retry.");
                }
                return;
            }
            // same as processFile(): MoveAction uses this flag to choose rename or copy-and-delete
            file.setIsMounted(isMounted);
            if (VFSConstants.FAILED_ACTION_SUCCESS.equals(record.action)) {
                postProcessingHandler.onSuccess(file);
            } else {
                postProcessingHandler.onFail(file);
            }
            Utils.releaseFailRecord(fsManager, file, fso);
            log.info("Retried the post-processing action of failed record " + maskURLPassword(file.toString())
                    + " successfully");
        } catch (Exception e) {
            // Rewrite the marker with the current time, so that the next attempt waits for the retry duration
            Utils.markFailRecord(fsManager, file, fso, record.action);
            log.warn("Retrying the " + (VFSConstants.FAILED_ACTION_SUCCESS.equals(record.action) ? "success"
                    : "failure") + " action of failed record " + maskURLPassword(file.toString()) + " failed: "
                    + e.getMessage() + ". It will be retried after " + failedRecordNextRetryDuration + " ms.");
            if (log.isDebugEnabled()) {
                log.debug("Retry failure of " + maskURLPassword(file.toString()), e);
            }
        } finally {
            if (locking) {
                lockManager.releaseLock(file);
            }
        }
    }

    private void warnUnknownFailRecord(FileObject file) {
        log.warn("File " + maskURLPassword(file.toString()) + " has a fail record from an earlier version, so the "
                + "action that failed is unknown and it is not retried. Handle the file manually and delete "
                + maskURLPassword(Utils.getFullPath(file)) + VFSConstants.FAIL_FILE_SUFFIX + " to clear it.");
    }

    private boolean exists(FileObject file) {
        try {
            return file.exists();
        } catch (FileSystemException e) {
            return false;
        }
    }

    /**
     * Creates the scheduler for failed record retries. On shutdown, retries that have not started yet are dropped
     * (they are scheduled again by the next poll after the endpoint is resumed), so closing the endpoint does not
     * wait for them; a retry that is already running is allowed to finish.
     */
    private static ScheduledExecutorService newRetryScheduler() {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return scheduler;
    }

    /**
     * Schedule retry for failed record processing
     */
    private void scheduleFailedRecordRetry(FileObject file) {
        // one pending retry per file, instead of a new task on every poll
        String uri = file.getName().getURI();
        if (failedRecordNextRetryDuration > 0 && pendingFailedRecordRetries.add(uri)) {
            try {
                retryScheduler.schedule(new FailedRecordRetryTask(file),
                        failedRecordNextRetryDuration, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // the endpoint is closing; the next poll after it is resumed schedules the retry again
                pendingFailedRecordRetries.remove(uri);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Scheduled retry for failed record: " + maskURLPassword(file.toString()) +
                        " after " + failedRecordNextRetryDuration + "ms");
            }
        }
    }

    /**
     * Moves a failed record to the MoveAfterFailedMove location and removes it from the failed records file.
     */
    private void removeFromFailedRecords(FileObject file) {
        try {
            if (vfsConfig.getMoveAfterMoveFailure() != null) {
                file.setIsMounted(isMounted);
                MoveAction moveAction = new MoveAction(vfsConfig.getMoveAfterMoveFailure(), vfsConfig, fsManager);
                moveAction.execute(file);
            }
            Utils.removeFailedRecord(vfsConfig, file);
            if (log.isDebugEnabled()) {
                log.debug("Removed file from failed records: " + maskURLPassword(file.toString()));
            }
        } catch (FileSystemException e) {
            VFSTransportErrorHandler.handleException(log, "Error moving file during failed record removal: " +
                    maskURLPassword(file.toString()), e);
        }
    }

    /**
     * Resolve hostname dynamically if enabled
     */
    private String resolveHostDynamically(String uri) {
        if (!resolveHostsDynamically) {
            return uri;
        }

        try {
            // Extract hostname from URI and resolve it
            // This is a simplified implementation
            if (uri.contains("://")) {
                String[] parts = uri.split("://");
                if (parts.length > 1) {
                    String[] hostParts = parts[1].split("/");
                    if (hostParts.length > 0) {
                        String host = hostParts[0];
                        if (host.contains("@")) {
                            host = host.substring(host.lastIndexOf("@") + 1);
                        }
                        if (host.contains(":")) {
                            host = host.substring(0, host.indexOf(":"));
                        }

                        // Resolve the hostname
                        InetAddress addr = InetAddress.getByName(host);
                        String resolvedIP = addr.getHostAddress();

                        if (log.isDebugEnabled()) {
                            log.debug("Resolved host " + host + " to " + resolvedIP);
                        }

                        // Replace hostname with resolved IP
                        return uri.replace(host, resolvedIP);
                    }
                }
            }
        } catch (UnknownHostException e) {
            log.warn("Could not resolve host dynamically for URI: " + maskURLPassword(uri), e);
        }

        return uri;
    }

    private void safeClose(FileObject fo) {
        if (fo != null) {
            try {
                fo.close();
            } catch (Exception ignore) {
            }
        }
    }

    /* =========================
                Helpers
       ========================= */

    private ResolvedFileUri extractFileUri(String propertyForUri) {
        String definedFileUri;
        switch (propertyForUri) {
            case VFSConstants.TRANSPORT_FILE_FILE_URI:
                definedFileUri = vfsConfig.getFileURI();
                break;
            default:
                definedFileUri = null;
        }
        if (StringUtils.isNotEmpty(definedFileUri)) {
            if (Utils.supportsSubDirectoryToken(definedFileUri)) {
                return new ResolvedFileUri(Utils.sanitizeFileUriWithSub(definedFileUri), true);
            } else {
                return new ResolvedFileUri(definedFileUri, false);
            }
        }
        return null;
    }

    /**
     * Check if the file/folder exists before proceeding and retrying
     */
    private FileObject initFileCheck(String fileURI) {
        boolean wasError = true;
        int retryCount = 0;

        FileObject fileObject = null;
        while (wasError) {
            try {
                if (isClosed) {
                    return null;
                }
                retryCount++;
                fileObject = fsManager.resolveFile(fileURI, fso);
                if (fileObject == null) {
                    log.error("fileObject is null");
                    throw new FileSystemException("fileObject is null");
                }
                Map<String, String> queryParams = UriParser.extractQueryParams(fileURI);
                isMounted = Boolean.parseBoolean(queryParams.get(VFSConstants.IS_MOUNTED));
                fileObject.setIsMounted(isMounted);
                wasError = false;
            } catch (FileSystemException e) {
                if (retryCount >= vfsConfig.getMaxRetryCount()) {
                    log.error("Repeatedly failed to resolve the file URI: " + maskURLPassword(fileURI), e);
                    return null;
                } else {
                    log.warn("Failed to resolve the file URI: " + maskURLPassword(fileURI) + ", in attempt "
                            + retryCount + ", " + e.getMessage() + " Retrying in " + vfsConfig.getReconnectTimeout()
                            + " milliseconds.");
                }
            }
            if (wasError) {
                try {
                    Thread.sleep(vfsConfig.getReconnectTimeout());
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                    log.error("Thread was interrupted while waiting to reconnect.", e2);
                }
            }
        }
        return fileObject;
    }

    public void close() {
        isClosed = true;
        fsManager.close();
        // retries still waiting are dropped below; the next poll after resume() schedules them again
        pendingFailedRecordRetries.clear();
        if (retryScheduler != null && !retryScheduler.isShutdown()) {
            retryScheduler.shutdown();
            try {
                if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    retryScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                retryScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public void start() {
        isClosed = false;
    }

    public void destroy() {
        this.close();
    }

    @Override
    public void resume() {
        try {
            ((StandardFileSystemManager) fsManager).init();
            pendingFailedRecordRetries.clear();
            retryScheduler = newRetryScheduler();
            isClosed = false;
        } catch (FileSystemException e) {
            log.error("Error re-initializing VFS FileSystemManager on resume", e);
        }
    }

    @Override
    public void pause() {
        isClosed = true;
        this.close();
    }

    private static class ResolvedFileUri {
        final String resolvedUri;
        final boolean supportSubDirectories;

        ResolvedFileUri(String uri, boolean subDirs) {
            this.resolvedUri = uri;
            this.supportSubDirectories = subDirs;
        }
    }

    /**
     * Task to retry processing of failed records
     */
    private class FailedRecordRetryTask implements Runnable {
        private final FileObject file;

        public FailedRecordRetryTask(FileObject file) {
            this.file = file;
        }

        @Override
        public void run() {
            boolean retryLater = false;
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Retrying failed record: " + maskURLPassword(file.toString()));
                }

                // Check if file still exists
                if (file.exists()) {
                    // Remove from failed records and retry processing
                    removeFromFailedRecords(file);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed record file no longer exists: " + maskURLPassword(file.toString()));
                    }
                    // the entry would otherwise block a later file with the same name
                    Utils.removeFailedRecord(vfsConfig, file);
                }
            } catch (Exception e) {
                log.error("Error during failed record retry: " + maskURLPassword(file.toString()), e);
                retryLater = true;
            } finally {
                pendingFailedRecordRetries.remove(file.getName().getURI());
            }
            if (retryLater) {
                // Schedule another retry if configured
                scheduleFailedRecordRetry(file);
            }
        }
    }

}

