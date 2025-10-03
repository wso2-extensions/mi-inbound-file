package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;

public class AgeFilter implements Filter {
    private Log log = LogFactory.getLog(AgeFilter.class);

    VFSConfig vfsConfig;
    public AgeFilter(VFSConfig vfsConfig) {
        this.vfsConfig = vfsConfig;
    }

    @Override
    public boolean accept(FileObject fileObject) {
        boolean isAccepted = false;
        if (vfsConfig.getMinimumAge() != null) {
            long age = 0;
            try {
                age = fileObject.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                log.info("Error in getting the last modified time of the file");
                return false;
            }
            long time = System.currentTimeMillis();
            isAccepted = (time - age) / 1000 <= vfsConfig.getMinimumAge();
        }
        if (vfsConfig.getMaximumAge() != null) {
            long age = 0;
            try {
                age = fileObject.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                log.info("Error in getting the last modified time of the file");
                return false;
            }
            long time = System.currentTimeMillis();
            isAccepted = (time - age) / 1000 >= vfsConfig.getMaximumAge();
        }
        return isAccepted;
    }



}
