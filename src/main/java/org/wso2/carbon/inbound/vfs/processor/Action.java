package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.vfs2.FileObject;

public interface Action {

    void execute(FileObject fileObject) throws Exception;
}
