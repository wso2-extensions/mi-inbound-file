package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;

import java.text.DateFormat;

import static org.wso2.carbon.inbound.vfs.Utils.sanitizeFileUriWithSub;

public class MoveAction implements Action {

    String targetLocation;
    VFSConfig vfsConfig;

    public MoveAction(String targetLocation, VFSConfig vfsConfig) {
        this.targetLocation = targetLocation;
        this.vfsConfig = vfsConfig;
    }

    private final Log log = LogFactory.getLog(MoveAction.class);

    @Override
    public void execute(FileObject fileObject) {
        // after success
        try {
            if (targetLocation != null && targetLocation.trim().length() > 0) {
                if (Utils.supportsSubDirectoryToken(targetLocation)) {
                    targetLocation = sanitizeFileUriWithSub(targetLocation);
                }
                log.info("Moving file to " + targetLocation);

                if (vfsConfig.getSubfolderTimestamp() != null) {
                    targetLocation = Utils.optionallyAppendDateToUri(targetLocation, vfsConfig);
                }
                FileObject dest = fileObject.getFileSystem().resolveFile(targetLocation + "/" + fileObject.getName().getBaseName());
                if ( vfsConfig.isUpdateLastModified()) {
                    dest.setUpdateLastModified(vfsConfig.isUpdateLastModified());
                }
                if (vfsConfig.getMoveTimestampFormat() != null) {
                    DateFormat moveTimestampFormat = vfsConfig.getMoveTimestampFormat();
                    dest = fileObject.getFileSystem().resolveFile(targetLocation + "/" + moveTimestampFormat +
                            fileObject.getName().getBaseName());
                }
                if (vfsConfig.isForceCreateFolder()) {
                    fileObject.createFile();
                }

                fileObject.moveTo(dest);

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
