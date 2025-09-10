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
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.vfs.filter.FileSelector;
import org.wso2.carbon.inbound.vfs.processor.Action;
import org.wso2.carbon.inbound.vfs.processor.DeleteAction;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.carbon.inbound.vfs.processor.PostProcessingHandler;
import org.wso2.carbon.inbound.vfs.processor.PreProcessingHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

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

        // Handlers (wire your concrete actions here)
        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment, properties);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(VFSUtils.getActionAfterProcess(vfsConfig, actionAfterProcess));
        this.postProcessingHandler.setOnFailAction(VFSUtils.getActionAfterProcess(vfsConfig, vfsConfig.getActionAfterFailure()));
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

        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment, properties);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(VFSUtils.getActionAfterProcess(vfsConfig, actionAfterProcess));
        this.postProcessingHandler.setOnFailAction(VFSUtils.getActionAfterProcess(vfsConfig, vfsConfig.getActionAfterFailure()));
        this.postProcessingHandler.setOnFailActionFailAction(new MoveAction(vfsConfig.getMoveAfterFailure()));
        this.postProcessingHandler.setOnSuccessActionFailAction(new MoveAction(vfsConfig.getMoveAfterMoveFailure()));

        this.fileSelector = new FileSelector(vfsConfig, fsManager);
        this.name = name;
    }

    @Override
    public Object poll() {
        // Resolve input URI and subdirectory setting from config (supports /* or \*)
        ResolvedFileUri inFileUri = extractFileUri(VFSConstants.TRANSPORT_FILE_FILE_URI);
        if (inFileUri == null || StringUtils.isBlank(inFileUri.resolvedUri)) {
            log.error("Invalid FileURI. Check configuration. URI: " + VFSUtils.maskURLPassword(vfsConfig.getFileURI()));
            return null;
        }
        readSubDirectories = inFileUri.supportSubDirectories;
        String fileURI = stripVfsSchemeIfPresent(inFileUri.resolvedUri);

        if (log.isDebugEnabled()) {
            log.debug("Polling VFS location: " + VFSUtils.maskURLPassword(fileURI) +
                    " (recursive=" + readSubDirectories + ")");
        }

        try {
            // Attach per-scheme options (SFTP, FTP, SMB, etc.)
            fso = VFSUtils.attachFileSystemOptions(vfsConfig.getVfsSchemeProperties(), fsManager);
        } catch (Exception e) {
            log.warn("Unable to attach scheme options for: " + VFSUtils.maskURLPassword(fileURI), e);
            fso = null; // continue; many schemes work without explicit options
        }

        FileObject root;
        try {
            root = fsManager.resolveFile(fileURI, fso);
        } catch (FileSystemException e) {
            log.error("Failed to resolve FileURI: " + VFSUtils.maskURLPassword(fileURI), e);
            return null;
        }

        if (root == null) {
            log.error("Resolved FileObject is null for: " + VFSUtils.maskURLPassword(fileURI));
            return null;
        }

        try {
            if (!root.exists() || !root.isReadable()) {
                log.warn("File/Directory not accessible: " + VFSUtils.maskURLPassword(fileURI));
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
                    log.debug("Ignoring non-file, non-folder: " + VFSUtils.maskURLPassword(root.toString()));
                }
            }
        } catch (FileSystemException e) {
            log.error("Error during poll for: " + VFSUtils.maskURLPassword(fileURI), e);
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
            log.error("Unable to list children of: " + VFSUtils.maskURLPassword(dir.toString()), e);
        }

        if (children == null || children.length == 0) {
            if (log.isDebugEnabled()) {
                log.debug("Empty directory: " + VFSUtils.maskURLPassword(dir.toString()));
            }
            return;
        }
        // 1. sort to ensure files are processed in order
        String fileSortParam = vfsConfig.getFileSortParam();
        if (StringUtils.isNotEmpty(fileSortParam)) {
            VFSUtils.sortFileObjects(children, fileSortParam, vfsConfig);
        }
        for (FileObject child : children) {
            try {
                // Skip lock/fail markers
                String base = child.getName().getBaseName();
                if (base.endsWith(".lock") || base.endsWith(".fail")) {
                    continue;
                }

                if (child.getType() == FileType.FOLDER) {
                    if (readSubDirectories) {
                        // Recurse only if enabled
                        processDirectory(child);
                    } else if (log.isDebugEnabled()) {
                        log.debug("Skipping subdirectory (recursive disabled): " +
                                VFSUtils.maskURLPassword(child.toString()));
                    }
                } else if (child.getType() == FileType.FILE) {
                    processFile(child);
                } else if (log.isDebugEnabled()) {
                    log.debug("Ignoring item (not file/folder): " +
                            VFSUtils.maskURLPassword(child.toString()));
                }
            } catch (Exception e) {
                // never block remaining files on a single failure
                log.error("Error processing child: " + VFSUtils.maskURLPassword(child.toString()), e);
            } finally {
                safeClose(child);
            }
        }
    }

    private void processFile(FileObject file) throws FileSystemException {
        // Delegate readiness, age, size, pattern, etc. to FileSelector
        if (!fileSelector.isValidFile(file)) {
            if (log.isDebugEnabled()) {
                log.debug("File not eligible: " + VFSUtils.maskURLPassword(file.toString()));
            }
            return;
        }

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
                postProcessingHandler.onSuccess(file);
            } else {
                postProcessingHandler.onFail(file);
            }
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
            if (VFSUtils.supportsSubDirectoryToken(definedFileUri)) {
                return new ResolvedFileUri(VFSUtils.sanitizeFileUriWithSub(definedFileUri), true);
            } else {
                return new ResolvedFileUri(definedFileUri, false);
            }
        }
        return null;
    }
}

