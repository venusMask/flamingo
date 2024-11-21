package org.apache.flamingo.file;

import org.apache.flamingo.options.Options;
import org.apache.flamingo.utils.Pair;
import org.apache.flamingo.writer.VLogWriter;

public class NamedUtil {

    public static String getDataDir() {
        return Options.DataDir.getValue();
    }

    public static String getValueDir() {
        return getDataDir() + "/value";
    }

    public static String getKeyDir() {
        return getDataDir() + "/key";
    }

    public static String getMetaDir() {
        return getDataDir() + "/meta/meta.json";
    }

    public static Pair<String, Long> getValueFilePath() {
        long millis = System.currentTimeMillis();
        String fileName = getValueDir() + "/" + String.format(VLogWriter.ACTIVE, millis);
        return Pair.of(fileName, millis);
    }

    public static Pair<String, Long> getKeyFilePath() {
        long millis = System.currentTimeMillis();
        String fileName = getKeyDir() + "/" + millis + ".sst";
        return Pair.of(fileName, millis);
    }


}
