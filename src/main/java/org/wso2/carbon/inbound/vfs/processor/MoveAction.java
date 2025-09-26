package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.carbon.inbound.vfs.VFSConstants;

import java.text.DateFormat;
import java.util.Map;

import static org.apache.commons.vfs2.provider.UriParser.extractQueryParams;
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
    public void execute(FileObject fileObject) throws FileSystemException {
        // after success

        if (targetLocation != null && targetLocation.trim().length() > 0) {
            if (Utils.supportsSubDirectoryToken(targetLocation)) {
                targetLocation = sanitizeFileUriWithSub(targetLocation);
            }

            log.info("Moving file to " + targetLocation);

            if (vfsConfig.getSubfolderTimestamp() != null) {
                targetLocation = Utils.optionallyAppendDateToUri(targetLocation, vfsConfig);
            }
            targetLocation = Utils.extractPath(targetLocation);

            FileObject dest = fileObject.getFileSystem().resolveFile(targetLocation + "/" + fileObject.getName().getBaseName());

            Map<String, String> queryParams = vfsConfig.getVfsSchemeProperties();
            if (!StringUtils.isEmpty(queryParams.get(org.wso2.carbon.inbound.vfs.VFSConstants.FORCE_CREATE_FOLDER))) {
                String isForceCreated = queryParams.get(VFSConstants.FORCE_CREATE_FOLDER);
                if (Boolean.parseBoolean(isForceCreated)) {
                    dest.createFile();
                }
            }
            if (vfsConfig.isUpdateLastModified()) {
                dest.setUpdateLastModified(vfsConfig.isUpdateLastModified());
            }
            if (vfsConfig.getMoveTimestampFormat() != null) {
                DateFormat moveTimestampFormat = vfsConfig.getMoveTimestampFormat();
                dest = fileObject.getFileSystem().resolveFile(targetLocation + "/" + moveTimestampFormat +
                        fileObject.getName().getBaseName());
            }


            fileObject.moveTo(dest);
            log.info("File moved");

        } else {
            log.info("Target location not provided. Hence not moving the file");
        }
    }

}
