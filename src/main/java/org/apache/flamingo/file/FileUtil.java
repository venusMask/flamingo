package org.apache.flamingo.file;

import org.apache.flamingo.core.Core;
import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.options.Options;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtil {

    public static String getSSTFilePath() {
        return Options.DATA_DIR.getValue() +
                File.separator + "sstable_" +
                IDAssign.getSSTNextID() + ".sst";
    }

    public static String getWalActiveName() {
        return getDataDirFilePath("wal_active_" + IDAssign.getWALNextID() + ".wal");
    }

    public static String getDataDirFilePath(String fileName) {
        return Options.DATA_DIR.getValue() + File.separator + fileName;
    }

    public static boolean deleteDataDirFile(String fileName) {
        String filePath = getDataDirFilePath(fileName);
        try {
            Files.deleteIfExists(Paths.get(filePath));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static boolean renameDataDirFile(String oldFileName, String newFileName) {
        try {
            File file = new File(getDataDirFilePath(oldFileName));
            return file.renameTo(new File(getDataDirFilePath(newFileName)));
        } catch (Exception e) {
            return false;
        }
    }

}
