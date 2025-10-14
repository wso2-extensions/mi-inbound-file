package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.vfs.VFSUtils;
import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.carbon.inbound.vfs.VFSConstants;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;

import java.text.DateFormat;
import java.util.Map;
import java.util.Properties;

import static org.wso2.carbon.inbound.vfs.Utils.extractPath;
import static org.wso2.carbon.inbound.vfs.Utils.sanitizeFileUriWithSub;

public class MoveAction implements Action {

    String targetLocation;
    VFSConfig vfsConfig;
    FileSystemManager fsManager;

    public MoveAction(String targetLocation, VFSConfig vfsConfig, FileSystemManager fsManager) {
        this.targetLocation = targetLocation;
        this.vfsConfig = vfsConfig;
        this.fsManager = fsManager;
    }

    private final Log log = LogFactory.getLog(MoveAction.class);

    @Override
    public void execute(FileObject fileObject) throws FileSystemException {
        // after success
        FileSystemOptions destinationFSO = null;
        Map<String, String> query = VFSUtils.parseSchemeFileOptions(targetLocation, new Properties());
        query.putAll(vfsConfig.getVfsSchemeProperties());
        targetLocation = extractPath(targetLocation);
        try {
            destinationFSO = Utils.attachFileSystemOptions(query, fsManager);
        } catch (Exception e) {
            log.warn("Unable to set the options for processed file location ", e);
        }

        if (targetLocation != null && targetLocation.trim().length() > 0) {
            if (Utils.supportsSubDirectoryToken(targetLocation)) {
                targetLocation = sanitizeFileUriWithSub(targetLocation);
            }

            log.info("Moving file to " + targetLocation);

            if (vfsConfig.getSubfolderTimestamp() != null) {
                targetLocation = Utils.optionallyAppendDateToUri(targetLocation, vfsConfig);
            }

            FileObject moveToDirectory = fsManager.resolveFile(targetLocation, destinationFSO);
            FileObject dest = moveToDirectory.resolveFile(fileObject.getName().getBaseName());
            if (!StringUtils.isEmpty(query.get(org.wso2.carbon.inbound.vfs.VFSConstants.FORCE_CREATE_FOLDER))) {
                String isForceCreated = query.get(VFSConstants.FORCE_CREATE_FOLDER);
                if (Boolean.parseBoolean(isForceCreated)) {
                    dest.createFile();
                }
            }

            dest.setUpdateLastModified(vfsConfig.isUpdateLastModified());

            if (vfsConfig.getMoveTimestampFormat() != null) {
                DateFormat moveTimestampFormat = vfsConfig.getMoveTimestampFormat();
                dest = fileObject.resolveFile( moveTimestampFormat + "_" + fileObject.getName().getBaseName());
            }

            fileObject.moveTo(dest);
            // Manually update last-modified time if config allows
            if (vfsConfig.isUpdateLastModified()) {
                try {
                    long srcModifiedTime = fileObject.getContent().getLastModifiedTime();
                    dest.getContent().setLastModifiedTime(srcModifiedTime);
                } catch (Exception e) {
                    log.warn("Failed to preserve last modified timestamp", e);
                }
            }
            log.info("File moved");

        } else {
            log.info("Target location not provided. Hence not moving the file");
        }
    }
}
