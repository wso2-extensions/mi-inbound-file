package org.wso2.carbon.inbound.vfs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.synapse.commons.vfs.VFSParamDTO;
import org.apache.synapse.commons.vfs.VFSUtils;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.vfs.filter.FileSelector;
import org.wso2.carbon.inbound.vfs.lock.LockManager;
import org.wso2.carbon.inbound.vfs.processor.Action;
import org.wso2.carbon.inbound.vfs.processor.DeleteAction;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.carbon.inbound.vfs.processor.PostProcessingHandler;
import org.wso2.carbon.inbound.vfs.processor.PreProcessingHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.synapse.commons.vfs.VFSUtils.*;

public class VFSConsumer extends GenericPollingConsumer {

    private static final Log log = LogFactory.getLog(VFSConsumer.class);
    private boolean fileLock = true;
    private final VFSConfig vfsConfig;
    private final FileSystemManager fsManager;
    private FileSystemOptions fso;

    private final FileSelector fileSelector;
    private final PreProcessingHandler preProcessingHandler;
    private final PostProcessingHandler postProcessingHandler;
    private final FileInjectHandler fileInjectHandler;

    private boolean readSubDirectories = false;
    private final String name;

    // File locking related fields
    private boolean autoLockRelease;
    private Boolean autoLockReleaseSameNode;
    private Long autoLockReleaseInterval;
    private boolean distributedLock;
    private Long distributedLockTimeout;

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
            mgr.setConfiguration(getClass().getClassLoader().getResource("providers.xml"));
            mgr.init();
            this.fsManager = mgr;
        } catch (FileSystemException e) {
            throw new RuntimeException("Error initializing VFS FileSystemManager", e);
        }

        // Build FSO once per poll (protocol options; auth; SFTP opts; etc.)
        this.fso = null;

        // Initialize file locking parameters
        initializeFileLockingParams(properties);

        // Handlers (wire your concrete actions here)
        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment, properties);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(Utils.getActionAfterProcess(vfsConfig, actionAfterProcess));
        this.postProcessingHandler.setOnFailAction(Utils.getActionAfterProcess(vfsConfig, vfsConfig.getActionAfterFailure()));
        this.postProcessingHandler.setOnFailActionFailAction(new MoveAction(vfsConfig.getMoveAfterFailure()));
        this.postProcessingHandler.setOnSuccessActionFailAction(new MoveAction(vfsConfig.getMoveAfterMoveFailure()));

        this.fileSelector = new FileSelector(vfsConfig, fsManager);
        this.name = name;
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
            mgr.setConfiguration(getClass().getClassLoader().getResource("providers.xml"));
            mgr.init();
            this.fsManager = mgr;
        } catch (FileSystemException e) {
            throw new RuntimeException("Error initializing VFS FileSystemManager", e);
        }

        this.fso = null;

        // Initialize file locking parameters
        initializeFileLockingParams(properties);

        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment, properties);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(Utils.getActionAfterProcess(vfsConfig, actionAfterProcess));
        this.postProcessingHandler.setOnFailAction(Utils.getActionAfterProcess(vfsConfig, vfsConfig.getActionAfterFailure()));
        this.postProcessingHandler.setOnFailActionFailAction(new MoveAction(vfsConfig.getMoveAfterFailure()));
        this.postProcessingHandler.setOnSuccessActionFailAction(new MoveAction(vfsConfig.getMoveAfterMoveFailure()));

        this.fileSelector = new FileSelector(vfsConfig, fsManager);
        this.name = name;
    }

    /**
     * Initialize file locking related parameters from properties
     */
    private void initializeFileLockingParams(Properties properties) {
        // Check if file locking is enabled
        String strFileLock = properties.getProperty(VFSConstants.TRANSPORT_FILE_LOCKING);
        if (strFileLock != null && strFileLock.toLowerCase().equals(VFSConstants.TRANSPORT_FILE_LOCKING_DISABLED)) {
            fileLock = false;
        }

        // Auto lock release configuration
        autoLockRelease = vfsConfig.isAutoLockRelease();
        if (autoLockRelease) {
            autoLockReleaseInterval = vfsConfig.getAutoLockReleaseInterval();
            autoLockReleaseSameNode = vfsConfig.getAutoLockReleaseSameNode();
        }

        // Distributed lock configuration
        distributedLock = false;
        distributedLockTimeout = null;
        String strDistributedLock = properties.getProperty(VFSConstants.TRANSPORT_DISTRIBUTED_LOCK);
        if (strDistributedLock != null) {
            try {
                distributedLock = Boolean.parseBoolean(strDistributedLock);
            } catch (Exception e) {
                distributedLock = false;
                log.warn("VFS Distributed lock not set properly. Current value is : " + strDistributedLock, e);
            }
            if (distributedLock) {
                String strDistributedLockTimeout = properties.getProperty(VFSConstants.TRANSPORT_DISTRIBUTED_LOCK_TIMEOUT);
                if (strDistributedLockTimeout != null) {
                    try {
                        distributedLockTimeout = Long.parseLong(strDistributedLockTimeout);
                    } catch (Exception e) {
                        distributedLockTimeout = null;
                        log.warn("VFS Distributed lock timeout property not set properly. Current value is : "
                                + strDistributedLockTimeout, e);
                    }
                }
            }
        }
    }

    @Override
    public Object poll() {
        // Resolve input URI and subdirectory setting from config (supports /* or \*)
        ResolvedFileUri inFileUri = extractFileUri(VFSConstants.TRANSPORT_FILE_FILE_URI);
        if (inFileUri == null || StringUtils.isBlank(inFileUri.resolvedUri)) {
            log.error("Invalid FileURI. Check configuration. URI: " + maskURLPassword(vfsConfig.getFileURI()));
            return null;
        }
        readSubDirectories = inFileUri.supportSubDirectories;
        String fileURI = stripVfsSchemeIfPresent(inFileUri.resolvedUri);

        if (log.isDebugEnabled()) {
            log.debug("Polling VFS location: " + maskURLPassword(fileURI) +
                    " (recursive=" + readSubDirectories + ")");
        }

        try {
            // Attach per-scheme options (SFTP, FTP, SMB, etc.)
            fso = attachFileSystemOptions(vfsConfig.getVfsSchemeProperties(), fsManager);
        } catch (Exception e) {
            log.warn("Unable to attach scheme options for: " + maskURLPassword(fileURI), e);
            fso = null; // continue; many schemes work without explicit options
        }

        FileObject root;
        try {
            root = fsManager.resolveFile(fileURI, fso);
        } catch (FileSystemException e) {
            log.error("Failed to resolve FileURI: " + maskURLPassword(fileURI), e);
            return null;
        }

        if (root == null) {
            log.error("Resolved FileObject is null for: " + maskURLPassword(fileURI));
            return null;
        }

        try {
            if (!root.exists() || !root.isReadable()) {
                log.warn("File/Directory not accessible: " + maskURLPassword(fileURI));
                return null;
            }

            if (root.getType() == FileType.FILE) {
                // Single-file mode
                processFile(root);
            } else if (root.getType() == FileType.FOLDER) {
                // Directory mode (optional recursion)
                processDirectory(root);
            } else {
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
        FileObject[] children = null;
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
            try {
                // Skip lock/fail markers
                String base = child.getName().getBaseName();
                if (base.endsWith(".lock") || base.endsWith(".fail")) {
                    continue;
                }

                // Check if this is a failed record
                boolean isFailedRecord = isFailRecord(fsManager, child, fso);
                if (isFailedRecord) {
                    // Handle failed record
                    handleFailedRecord(child);
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
                    processFile(child);
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
        // Delegate readiness, age, size, pattern, etc. to FileSelector
        if (!fileSelector.isValidFile(file)) {
            if (log.isDebugEnabled()) {
                log.debug("File not eligible: " + maskURLPassword(file.toString()));
            }
            return;
        }

        // Acquire lock if file locking is enabled
        LockManager lockManager = new LockManager(fileLock, autoLockRelease,
                autoLockReleaseSameNode, autoLockReleaseInterval,
                distributedLock,
                fsManager, fso);

        if (fileLock && !lockManager.acquireLock(file)) {
            log.error("Couldn't get the lock for processing the file: " +
                    maskURLPassword(file.getName().toString()));
            return;
        }

        boolean processSuccessful = false;
        boolean skipUnlock = false;

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

                try {
                    headers.put(org.apache.synapse.commons.vfs.VFSConstants.FILE_LENGTH, content.getSize());
                    headers.put(org.apache.synapse.commons.vfs.VFSConstants.LAST_MODIFIED, content.getLastModifiedTime());
                } catch (FileSystemException ignore) {
                    // length/mtime are best-effort
                }

                fileInjectHandler.setTransportHeaders(headers);
                fileInjectHandler.setFileURI(fileURI);

                boolean ok = fileInjectHandler.invoke(file, name);
                if (ok) {
                    processSuccessful = true;
                    postProcessingHandler.onSuccess(file);
                } else {
                    postProcessingHandler.onFail(file);
                }
            }
        } catch (Exception e) {
            log.error("Error processing file: " + maskURLPassword(file.toString()), e);
            try {
                postProcessingHandler.onFail(file);
            } catch (Exception failHandlingError) {
                log.error("Error in fail handling for file: " + maskURLPassword(file.toString()), failHandlingError);
                // Mark as failed record if we couldn't handle the failure
                markFailRecord(fsManager, file, fso);
                skipUnlock = true;
            }
        } finally {
            // Release lock if file locking is enabled and we shouldn't skip
            if (fileLock && !skipUnlock) {
                releaseLock(fsManager, file, fso);
                if (log.isDebugEnabled()) {
                    log.debug("Released the lock for file: " + maskURLPassword(file.toString()));
                }
            }
        }
    }

    /**
     * Handle failed records - attempt to process them again
     */
    private void handleFailedRecord(FileObject file) throws FileSystemException {
        try {
            postProcessingHandler.onSuccess(file);
        } catch (Exception e) {
            log.error("File object '" + maskURLPassword(file.getURL().toString()) +
                    "' could not be moved after first attempt", e);
        }

        if (fileLock) {
            releaseLock(fsManager, file, fso);
        }

        if (log.isDebugEnabled()) {
            log.debug("File '" + maskURLPassword(file.getURL().toString()) +
                    "' has been marked as a failed record, attempting to handle it");
        }
    }



    /* =========================
                Helpers
       ========================= */

    private String stripVfsSchemeIfPresent(String uri) {
        return uri != null && uri.startsWith("vfs:") ? uri.substring(4) : uri;
    }

    private void safeClose(FileObject fo) {
        if (fo != null) {
            try { fo.close(); } catch (Exception ignore) {}
        }
    }

    private static class ResolvedFileUri {
        final String resolvedUri;
        final boolean supportSubDirectories;
        ResolvedFileUri(String uri, boolean subDirs) {
            this.resolvedUri = uri; this.supportSubDirectories = subDirs;
        }
    }

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
}

