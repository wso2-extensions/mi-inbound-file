package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.vfs2.FileObject;
import org.wso2.carbon.inbound.vfs.VFSConfig;

public class SizeFilter implements Filter{
    VFSConfig vfsConfig;
    public SizeFilter(VFSConfig vfsConfig) {
        this.vfsConfig = vfsConfig;
    }

    @Override
    public boolean accept(FileObject fileObject) {
        try {
            return vfsConfig.getFileSizeLimit() >= 0 && fileObject.getContent().getSize() < vfsConfig.getFileSizeLimit();
        } catch (Exception e) {
            return false;
        }
    }
}
