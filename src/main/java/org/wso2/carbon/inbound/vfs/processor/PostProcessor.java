package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.vfs2.FileObject;

public interface PostProcessor {

    void onSuccess(FileObject fileObject);
    void onFail(FileObject fileObject);
    void onSuccessActionFail(FileObject fileObject);
    void onFailActionFail(FileObject fileObject);
}
