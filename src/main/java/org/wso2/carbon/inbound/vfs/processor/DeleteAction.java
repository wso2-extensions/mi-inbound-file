package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

public class DeleteAction implements Action{

    @Override
    public void execute(FileObject fileObject) throws FileSystemException {
        fileObject.delete();
    }
}
