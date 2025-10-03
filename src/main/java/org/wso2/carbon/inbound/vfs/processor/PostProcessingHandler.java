package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.org.apache.commons.vfs2.FileObject;

public class PostProcessingHandler implements PostProcessor {

    private Log log = LogFactory.getLog(PostProcessingHandler.class);
    private Action onSuccessAction;
    private Action onFailAction;

    @Override
    public void onSuccess(FileObject fileObject) throws Exception {

        if (onSuccessAction !=null) {
            log.info("Running on success action");
            onSuccessAction.execute(fileObject);
        }
    }

    @Override
    public void onFail(FileObject fileObject) throws Exception {
        if (onFailAction!=null) {
            log.info("Running on fail action");
            onFailAction.execute(fileObject);
        }
    }

    public void setOnSuccessAction(Action onSuccessAction) {
        this.onSuccessAction = onSuccessAction;
    }

    public void setOnFailAction(Action onFailAction) {

        this.onFailAction = onFailAction;
    }
}
