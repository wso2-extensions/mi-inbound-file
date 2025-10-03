package org.wso2.carbon.inbound.vfs.processor;


import org.wso2.org.apache.commons.vfs2.FileObject;

public interface PostProcessor {

    void onSuccess(FileObject fileObject) throws Exception;
    void onFail(FileObject fileObject) throws Exception;
}
