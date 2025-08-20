package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.vfs2.FileObject;

public interface Filter {

    boolean accept(FileObject fileObject);
}
