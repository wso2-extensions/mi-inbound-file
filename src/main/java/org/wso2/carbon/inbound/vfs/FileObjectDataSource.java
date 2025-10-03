//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.wso2.carbon.inbound.vfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.axiom.attachments.SizeAwareDataSource;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;


public class FileObjectDataSource implements SizeAwareDataSource {
    private final FileObject file;
    private final String contentType;

    public FileObjectDataSource(FileObject file, String contentType) {
        this.file = file;
        this.contentType = contentType;
    }

    public long getSize() {
        try {
            return this.file.getContent().getSize();
        } catch (FileSystemException var2) {
            return -1L;
        }
    }

    public String getContentType() {
        return this.contentType;
    }

    public String getName() {
        return this.file.getName().getURI();
    }

    public InputStream getInputStream() throws IOException {
        return this.file.getContent().getInputStream();
    }

    public OutputStream getOutputStream() throws IOException {
        return this.file.getContent().getOutputStream();
    }
}
