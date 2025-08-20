package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;

public class MoveAction implements Action{

    private Log log = LogFactory.getLog(MoveAction.class);

    @Override
    public void execute(FileObject fileObject) {

        log.info("File moved");
    }
}
