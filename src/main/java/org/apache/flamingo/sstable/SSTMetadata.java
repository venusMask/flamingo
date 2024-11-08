package org.apache.flamingo.sstable;

import org.apache.flamingo.file.FileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

/**
 * Store SSTable MetaInfo.
 */
public class SSTMetadata {

    private final Map<Integer, List<SSTable>> metaData = new HashMap<>();

    private final String metaFilePath;

    public SSTMetadata() {
        this.metaFilePath = FileUtil.getMetaInfoPath();
        init();
    }

    public void addFirstLevel(SSTable ssTable) {
        List<SSTable> list = metaData.computeIfAbsent(0, k -> new ArrayList<>());
        list.add(ssTable);
        flushMetadata();
    }

    private void flushMetadata() {
        try {
            FileOutputStream outputStream = new FileOutputStream(metaFilePath);
            metaData.values().forEach(list -> list.forEach(ssTable -> {
                try {
                    outputStream.write(SSTable.serialize(ssTable));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
            outputStream.close();
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private void init() {
        File metaInfoFile = new File(metaFilePath);
        try {
            if (metaInfoFile.exists()) {
                try (BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(metaInfoFile.toPath()))) {
                    byte[] fileBytes = new byte[(int) metaInfoFile.length()];
                    int read = inputStream.read(fileBytes);
                    if (read > 0) {
                        ByteBuffer byteBuffer = ByteBuffer.wrap(fileBytes);
                        while (byteBuffer.hasRemaining()) {
                            SSTable ssTable = SSTable.deserialize(byteBuffer);
                            int level = ssTable.getLevel();
                            metaData.computeIfAbsent(level, k -> new ArrayList<>()).add(ssTable);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading meta info file: " + metaFilePath, e);
        }
    }

}
