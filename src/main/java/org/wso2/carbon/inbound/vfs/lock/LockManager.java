package org.wso2.carbon.inbound.vfs.lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.synapse.commons.vfs.VFSParamDTO;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import static org.apache.synapse.commons.vfs.VFSUtils.maskURLPassword;

public class LockManager {
    private static final Log log = LogFactory.getLog(LockManager.class);

    private static final Random randomNumberGenerator = new Random();

    private boolean fileLock;
    private boolean autoLockRelease;
    private boolean autoLockReleaseSameNode;
    private long autoLockReleaseInterval;
    private boolean distributedLock;
//    private long distributedLockTimeout;
    private org.apache.commons.vfs2.FileSystemManager fsManager;
    private org.apache.commons.vfs2.FileSystemOptions fso;

    public LockManager(boolean fileLock, boolean autoLockRelease, boolean autoLockReleaseSameNode,
                       long autoLockReleaseInterval, boolean distributedLock,
                       org.apache.commons.vfs2.FileSystemManager fsManager, org.apache.commons.vfs2.FileSystemOptions fso) {
        this.fileLock = fileLock;
        this.autoLockRelease = autoLockRelease;
        this.autoLockReleaseSameNode = autoLockReleaseSameNode;
        this.autoLockReleaseInterval = autoLockReleaseInterval;
        this.distributedLock = distributedLock;
//        this.distributedLockTimeout = distributedLockTimeout;
        this.fsManager = fsManager;
        this.fso = fso;
    }

    /**
     * Acquire lock on file if required
     */
    public boolean acquireLock(FileObject fileObject) {
        if (!fileLock) {
            return true;
        }

        String strContext = fileObject.getName().getURI();

        // When processing a directory list is fetched initially. Therefore
        // there is still a chance of file processed by another process.
        // Need to check the source file before processing.
        try {
            String parentURI = fileObject.getParent().getName().getURI();
            if (parentURI.contains("?")) {
                String suffix = parentURI.substring(parentURI.indexOf("?"));
                strContext += suffix;
            }
            FileObject sourceFile = fsManager.resolveFile(strContext, fso);
            if (!sourceFile.exists()) {
                return false;
            }
        } catch (FileSystemException e) {
            return false;
        }

        // Create VFSParamDTO with lock parameters
        VFSParamDTO vfsParamDTO = new VFSParamDTO();
        vfsParamDTO.setAutoLockRelease(autoLockRelease);
        vfsParamDTO.setAutoLockReleaseSameNode(autoLockReleaseSameNode);
        vfsParamDTO.setAutoLockReleaseInterval(autoLockReleaseInterval);

        // Additional distributed lock parameters if needed
        if (distributedLock) {
            // Set distributed lock params if your VFSParamDTO supports them
            // vfsParamDTO.setDistributedLock(distributedLock);
            // vfsParamDTO.setDistributedLockTimeout(distributedLockTimeout);
        }

        return acquireLock(fsManager, fileObject, vfsParamDTO, fso, true);
    }



    public static synchronized boolean acquireLock(FileSystemManager fsManager, FileObject fo, VFSParamDTO paramDTO, FileSystemOptions fso, boolean isListener) {
        String strLockValue = getLockValue();
        byte[] lockValue = strLockValue.getBytes();
        FileObject lockObject = null;
        String fullPath = getFullPath(fo);

        try {
            lockObject = fsManager.resolveFile(fullPath + ".lock", fso);
            if (lockObject.exists()) {
                log.debug("There seems to be an external lock, aborting the processing of the file " + maskURLPassword(fo.getName().getURI()) + ". This could possibly be due to some other party already processing this file or the file is still being uploaded");
                if (paramDTO != null && paramDTO.isAutoLockRelease()) {
                    releaseLock(lockValue, strLockValue, lockObject, paramDTO.isAutoLockReleaseSameNode(), paramDTO.getAutoLockReleaseInterval());
                }
            } else {
                FileObject verifyingLockObject;
                if (isListener) {
                    verifyingLockObject = fsManager.resolveFile(fullPath, fso);
                    if (!verifyingLockObject.exists()) {
                        return false;
                    }
                }

                if (!createLockFile(lockValue, lockObject, fullPath)) {
                    return false;
                }

                verifyingLockObject = fsManager.resolveFile(fullPath + ".lock", fso);
                if (verifyingLockObject.exists() && verifyLock(lockValue, verifyingLockObject)) {
                    return true;
                }
            }
        } catch (FileSystemException var13) {
            log.error("Cannot get the lock for the file : " + maskURLPassword(fo.getName().getURI()) + " before processing", var13);
            if (lockObject != null) {
                try {
                    fsManager.closeFileSystem(lockObject.getParent().getFileSystem());
                } catch (FileSystemException var12) {
                    log.warn("Unable to close the lockObject parent file system");
                }
            } else {
                try {
                    ((DefaultFileSystemManager)fsManager).closeCachedFileSystem(fullPath + ".lock", fso);
                } catch (Exception var11) {
                    log.warn("Unable to clear file system", var11);
                }
            }
        }

        return false;
    }

    private static String getLockValue() {
        StringBuilder lockValueBuilder = new StringBuilder();
        lockValueBuilder.append(randomNumberGenerator.nextLong());

        try {
            lockValueBuilder.append(":").append(InetAddress.getLocalHost().getHostName()).append(":").append(InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException var2) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to get the Hostname or IP.", var2);
            }
        }

        lockValueBuilder.append(":").append((new Date()).getTime());
        return lockValueBuilder.toString();
    }

    private static String getFullPath(FileObject fo) {
        String fullPath = fo.getName().getURI();
        int pos = fullPath.indexOf(63);
        if (pos != -1) {
            fullPath = fullPath.substring(0, pos);
        }

        return fullPath;
    }

    private static void releaseLock(byte[] bLockValue, String sLockValue, FileObject lockObject, Boolean autoLockReleaseSameNode, Long autoLockReleaseInterval) {
        try {
            InputStream is = lockObject.getContent().getInputStream();
            byte[] val = new byte[bLockValue.length];
            is.read(val);
            is.close();
            String strVal = new String(val);
            String[] arrVal = strVal.split(":");
            String[] arrValNew = sLockValue.split(":");
            if (arrVal.length != 4 || arrValNew.length != 4 || autoLockReleaseSameNode && (!arrVal[1].equals(arrValNew[1]) || !arrVal[2].equals(arrValNew[2]))) {
                lockObject.close();
            } else {
                long lInterval = 0L;

                try {
                    lInterval = Long.parseLong(arrValNew[3]) - Long.parseLong(arrVal[3]);
                } catch (NumberFormatException var13) {
                    log.debug("Error calculating lock file age", var13);
                }

                deleteLockFile(lockObject, autoLockReleaseInterval, lInterval);
            }
        } catch (IOException var14) {
            log.error("Couldn't verify the lock", var14);
        }

    }


    private static boolean createLockFile(byte[] lockValue, FileObject lockObject, String fullPath) throws FileSystemException {
        lockObject.createFile();
        OutputStream stream = lockObject.getContent().getOutputStream();

        boolean var5;
        try {
            stream.write(lockValue);
            stream.flush();
            return true;
        } catch (IOException var15) {
            lockObject.delete();
            log.error("Couldn't create the lock file before processing the file " + maskURLPassword(fullPath), var15);
            var5 = false;
        } finally {
            try {
                stream.close();
            } catch (IOException var14) {
                log.debug("Error closing stream", var14);
            }

            lockObject.close();
        }

        return var5;
    }


    private static void deleteLockFile(FileObject lockObject, Long autoLockReleaseInterval, long lInterval) throws FileSystemException {
        if (autoLockReleaseInterval == null || autoLockReleaseInterval <= lInterval) {
            try {
                lockObject.delete();
            } catch (Exception var8) {
                log.warn("Unable to delete the lock file during auto release cycle.", var8);
            } finally {
                lockObject.close();
            }
        }

    }


    private static boolean verifyLock(byte[] lockValue, FileObject lockObject) {
        try {
            InputStream is = lockObject.getContent().getInputStream();
            byte[] val = new byte[lockValue.length];
            is.read(val);
            if (Arrays.equals(lockValue, val) && is.read() == -1) {
                return true;
            } else {
                log.debug("The lock has been acquired by an another party");
                return false;
            }
        } catch (IOException var4) {
            log.error("Couldn't verify the lock", var4);
            return false;
        }
    }
}
