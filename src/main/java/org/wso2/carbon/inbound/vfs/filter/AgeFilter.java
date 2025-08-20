package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.vfs2.FileObject;

public class AgeFilter implements Filter{

    @Override
    public boolean accept(FileObject fileObject) {

        return false;
    }
}
