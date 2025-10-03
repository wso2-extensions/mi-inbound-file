package org.wso2.carbon.inbound.vfs.filter;

import org.wso2.carbon.inbound.vfs.VFSConfig;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;

public class FileSelector {

    private final List<Filter> filters;
//    Log log = LogFactory.getlog(FileSelector.class.getName());

    public FileSelector(VFSConfig config, FileSystemManager fsManager) {
        filters = new ArrayList<>();
        if (config.getFileNamePattern() != null && !config.getFileNamePattern().isEmpty()) {
            filters.add(new FileNameFilter(config));
        }
        if (config.getFileSizeLimit() >= 0) {
            filters.add(new SizeFilter(config));
        }
        if (config.getCheckSizeInterval() != null && !config.getCheckSizeInterval().isEmpty()) {
            filters.add(new SizeCheckFilter(config, fsManager));
        }
        if ((config.getMaximumAge() != null && !config.getMaximumAge().toString().isEmpty()) ||
                (config.getMinimumAge()!= null && !config.getMinimumAge().toString().isEmpty())) {
            filters.add(new AgeFilter(config));
        }
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
