package org.wso2.carbon.inbound.vfs;

import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;

public class FileReader {

    private FileSystemManager fileSystemManager;
    private FileSystemOptions fileSystemOptions;
    private String fileURI;
    private int maxRetries = 3;
    private int reconnectionDelay = 1000;

    public FileReader(String fileURI, FileSystemManager fileSystemManager, FileSystemOptions fileSystemOptions) {

        this.fileURI = processFileURI(fileURI);
        this.fileSystemManager = fileSystemManager;
        this.fileSystemOptions = fileSystemOptions;
    }

    public FileObject readDirectoryWithRetry() {

        FileObject fileObject = null;
        boolean wasError = true;
        int retryCount = 0;
        do {
            try {
                fileObject = fileSystemManager.resolveFile(fileURI, fileSystemOptions);
                validateDirectory(fileObject);
                wasError = false;
            } catch (FileSystemException e) {
                if (retryCount >= maxRetries) {
                    // handle this properly
                    throw new RuntimeException("Failed to read directory after " + maxRetries + " attempts", e);
                }
                retryCount++;
            } catch (Exception e) {
                //log
                throw new RuntimeException("Unexpected error while reading directory: " + fileURI, e);
            }
            if (wasError) {
                try {
                    Thread.sleep(reconnectionDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupted status
                    throw new RuntimeException("Thread interrupted while waiting to retry", e);
                }
            }
        } while (wasError);
        return fileObject;
    }

    public void validateDirectory(FileObject fileObject) throws FileSystemException {

            if (fileObject == null) {
//                log.error("fileObject is null");
                throw new FileSystemException("fileObject is null");
            }
//            Map<String, String> queryParams = VFSUtils.extractQueryParams(fileURI);
//            fileObject.setIsMounted(Boolean.parseBoolean(queryParams.get(VFSConstants.IS_MOUNTED)));

            fileObject.exists();
    }


    private String processFileURI(String fileURI) {
        if(fileURI.contains("vfs:")){
            return fileURI.substring(fileURI.indexOf("vfs:") + 4);
        }
        return fileURI;
    }
}
