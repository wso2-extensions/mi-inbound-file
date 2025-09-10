package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.vfs2.FileObject;
import org.wso2.carbon.inbound.vfs.VFSConfig;

public class AgeFilter implements Filter {
    VFSConfig vfsConfig;
    public AgeFilter(VFSConfig vfsConfig) {
        this.vfsConfig = vfsConfig;
    }

    @Override
    public boolean accept(FileObject fileObject) {
        return false;
    }

}
