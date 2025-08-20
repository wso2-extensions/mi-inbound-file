package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.vfs2.FileObject;

import java.util.ArrayList;
import java.util.List;

public class FileSelector {

    private List<Filter> filters;

    public FileSelector() {

        filters = new ArrayList<>();
    }

    public boolean isValidFile(FileObject fileObject) {

        for (Filter filter : filters) {
            if (!filter.accept(fileObject)) {
                return false;
            }
        }
        return true;
    }
}
