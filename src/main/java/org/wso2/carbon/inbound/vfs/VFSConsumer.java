package org.wso2.carbon.inbound.vfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.vfs.filter.FileSelector;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.carbon.inbound.vfs.processor.PostProcessingHandler;
import org.wso2.carbon.inbound.vfs.processor.PreProcessingHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class VFSConsumer extends GenericPollingConsumer {

    private final Log log = LogFactory.getLog(VFSConsumer.class.getName());
    private Properties vfsProperties;
    private FileSystemManager fsManager;
    private FileSystemOptions fsOptions;
    private final VFSConfig vfsConfig;
    private String name;
    private boolean isFileSystemClosed = false;
    private PreProcessingHandler preProcessingHandler;
    private FileInjectHandler fileInjectHandler;
    private PostProcessingHandler postProcessingHandler;
    private FileSelector fileSelector;

    public VFSConsumer(Properties properties, String name,
                       SynapseEnvironment synapseEnvironment, long scanInterval,
                       String injectingSeq, String onErrorSeq, boolean coordination, boolean sequential) {

        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);
        this.name = name;
        vfsConfig = new VFSConfig(properties);
        fsManager = new StandardFileSystemManager();
        ((StandardFileSystemManager) fsManager).setConfiguration(getClass().getClassLoader().getResource("providers.xml"));
        try {
            ((StandardFileSystemManager) fsManager).init();
        } catch (FileSystemException e) {
            throw new RuntimeException("Error initializing VFS FileSystemManager", e);
        }
        fsOptions = new FileSystemOptions(); // TODO: load from properties
        fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment, properties);
        preProcessingHandler = new PreProcessingHandler(); // TODO: setup this
        postProcessingHandler = new PostProcessingHandler(); // TODO: setup this
        postProcessingHandler.setOnSuccessAction(new MoveAction());
        fileSelector = new FileSelector(); // TODO: setup this
    }

    @Override
    public Object poll() {


        if (log.isDebugEnabled()) {
            log.debug("Polling: " + VFSUtils.maskURLPassword(vfsConfig.getFileURI()));
        }

        if (vfsConfig.isClusterAware()) {
            if (log.isDebugEnabled()) {
                log.debug("Cluster aware flag is enabled.");
            }
        }

        FileSystemOptions fso = null;
        setFileSystemClosed(false);

        try {
            fso = VFSUtils.attachFileSystemOptions(vfsConfig.getVfsSchemeProperties(), fsManager);
        } catch (FileSystemException e) {
            throw new RuntimeException(e);
        }

        FileObject fileObject = null;

        String fileURI = vfsConfig.getFileURI();
        if(fileURI.contains("vfs:")){
            fileURI = fileURI.substring(fileURI.indexOf("vfs:") + 4);
        }

        FileReader fileReader = new FileReader(fileURI, fsManager, fso);
        fileObject = fileReader.readDirectoryWithRetry();
        if (fileObject == null) {
            log.error("FileObject is null for URI: " + VFSUtils.maskURLPassword(fileURI));
            return null;
        }
        try {
            if (fileObject.isFile()) {
                processFile(fileObject);
            } else if (fileObject.isFolder()) {
                processDirectory(fileObject);
            }
        } catch (FileSystemException e) {
            throw new RuntimeException(e);
        }

        if (log.isDebugEnabled()) {
            log.debug("Scanning directory or file : " + VFSUtils.maskURLPassword(fileURI));
        }

        return null;
    }

    private void processDirectory(FileObject fileObject) {

    }

    private void processFile(FileObject fileObject) throws FileSystemException {
        if (canProcessFile(fileObject)) {
            preProcessingHandler.handle(fileObject);

            FileContent content = fileObject.getContent();
            String fileName = fileObject.getName().getBaseName();
            String filePath = fileObject.getName().getPath();
            String fileURI = fileObject.getName().getURI();

            Map<String, Object> transportHeaders = new HashMap<>();
            transportHeaders.put(VFSConstants.FILE_NAME, fileName);
            transportHeaders.put(VFSConstants.FILE_PATH, filePath);
            transportHeaders.put(VFSConstants.FILE_URI, fileURI);

            try {
                transportHeaders.put(org.apache.synapse.commons.vfs.VFSConstants.FILE_LENGTH, content.getSize());
                transportHeaders.put(
                        org.apache.synapse.commons.vfs.VFSConstants.LAST_MODIFIED, content.getLastModifiedTime());
            } catch (FileSystemException ignore) {
//                closeFileSystem(file);
            }
            fileInjectHandler.setTransportHeaders(transportHeaders);
            fileInjectHandler.setFileURI(fileURI);
            boolean status = fileInjectHandler.invoke(fileObject, name);
            if (status) {
                postProcessingHandler.onSuccess(fileObject);
            } else {
                postProcessingHandler.onFail(fileObject);
            }
        }
    }

    private boolean canProcessFile(FileObject fileObject) {

        // check if the file is still uploading or being processed by another thread , check size limit, age
        return fileSelector.isValidFile(fileObject);
    }

    public void setFileSystemClosed(boolean fileSystemClosed) {

        isFileSystemClosed = fileSystemClosed;
    }

    private void closeFileSystem(FileObject fileObject) {
        try {
            //Close the File system if it is not already closed by the finally block of processFile method
            if (fileObject != null && fsManager != null && fileObject.getParent() != null  && fileObject.getParent().getFileSystem() != null) {
                fsManager.closeFileSystem(fileObject.getParent().getFileSystem());
                fileObject.close();
                setFileSystemClosed(true);
            }
        } catch (FileSystemException warn) {
            //  log.warn("Cannot close file after processing : " + file.getName().getPath(), warn);
            // ignore the warning, since we handed over the stream close job to AutocloseInputstream..
        }
    }
}
