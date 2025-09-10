package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.wso2.carbon.inbound.vfs.VFSConsumer;
import org.wso2.carbon.inbound.vfs.VFSUtils;

import static org.wso2.carbon.inbound.vfs.VFSUtils.sanitizeFileUriWithSub;

public class MoveAction implements Action {

    String targetLocation;

    public MoveAction(String targetLocation) {
        this.targetLocation = targetLocation;
    }

    private Log log = LogFactory.getLog(MoveAction.class);

    @Override
    public void execute(FileObject fileObject) {
        // after success
        try {
            if (targetLocation != null && targetLocation.trim().length() > 0) {
                if (VFSUtils.supportsSubDirectoryToken(targetLocation)) {
                    targetLocation = sanitizeFileUriWithSub(targetLocation);
                }
                log.info("Moving file to " + targetLocation);
                fileObject.moveTo(fileObject.getFileSystem().resolveFile(targetLocation + "/" + fileObject.getName().getBaseName()));

            } else {
                log.info("Target location not provided. Hence not moving the file");
                return;
            }
        } catch (FileSystemException e) {
            throw new RuntimeException(e);
        }
        log.info("File moved");
    }
}
