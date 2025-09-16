package org.wso2.carbon.inbound.vfs.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;

public class PostProcessingHandler implements PostProcessor {

    private Log log = LogFactory.getLog(PostProcessingHandler.class);
    private Action onSuccessAction;
    private Action onFailAction;
    private Action onFailActionFailAction;
    private Action onSuccessActionFailAction;

    @Override
    public void onSuccess(FileObject fileObject) {

        if (onSuccessAction !=null) {
            log.info("Running on success action");
            try {
                onSuccessAction.execute(fileObject);
            } catch (Exception e) {
                onSuccessActionFail(fileObject);
            }
        }
    }

    @Override
    public void onFail(FileObject fileObject) {
        if (onFailAction!=null) {
            log.info("Running on fail action");
            try {
                onFailAction.execute(fileObject);
            } catch (Exception e) {
                onFailActionFail(fileObject);
            }
        }
    }

    @Override
    public void onSuccessActionFail(FileObject fileObject) {

        if(onSuccessActionFailAction !=null) {
            try {
                onSuccessActionFailAction.execute(fileObject);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void onFailActionFail(FileObject fileObject) {
        if(onFailActionFailAction!=null){
            try {
                onFailActionFailAction.execute(fileObject);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void setOnSuccessAction(Action onSuccessAction) {
        this.onSuccessAction = onSuccessAction;
    }

    public void setOnFailAction(Action onFailAction) {

        this.onFailAction = onFailAction;
    }

    public void setOnFailActionFailAction(Action onFailActionFailAction) {

        this.onFailActionFailAction = onFailActionFailAction;
    }

    public void setOnSuccessActionFailAction(Action onSuccessActionFailAction) {

        this.onSuccessActionFailAction = onSuccessActionFailAction;
    }
}
