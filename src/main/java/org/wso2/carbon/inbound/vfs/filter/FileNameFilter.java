package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.vfs2.FileObject;
import org.wso2.carbon.inbound.vfs.VFSConfig;

import java.util.logging.Logger;

public class FileNameFilter implements Filter {
    Logger logger = Logger.getLogger(FileNameFilter.class.getName());
    VFSConfig vfsConfig;
    public FileNameFilter(VFSConfig vfsConfig) {
        this.vfsConfig = vfsConfig;
    }


    @Override
    public boolean accept(FileObject fileObject) {
        String fileNamePattern = vfsConfig.getFileNamePattern();
        //todo: Regex support
        return fileObject.getName().getBaseName().matches(fileNamePattern);
    }
}
