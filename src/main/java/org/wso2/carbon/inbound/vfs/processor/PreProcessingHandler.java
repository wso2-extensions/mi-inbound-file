package org.wso2.carbon.inbound.vfs.processor;

import org.wso2.org.apache.commons.vfs2.FileObject;

public class PreProcessingHandler {

    public void handle(FileObject fileObject) {
    // only one action to move the file to a different location (user provides) or do nothing
        // just call the move execute method in action
    }
}
