package org.wso2.carbon.inbound.vfs.filter;

import org.wso2.org.apache.commons.vfs2.FileObject;

public interface Filter {

    boolean accept(FileObject fileObject);

}
