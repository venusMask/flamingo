package org.apache.flamingo.wal;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.memtable.DefaultMemTable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

@Slf4j
@Getter
public class WALWriter {

    private final DefaultMemTable memTable;

    private final FileChannel writeChannel;

    private final String walActiveID;

    private final String walSilenceID;

    private final String walActiveFullPath;

    private final String walSilenceFullPath;

    public WALWriter(DefaultMemTable memTable) {
        this.memTable = memTable;
        this.walActiveID = getActiveID();
        this.walSilenceID = getSilenceID(walActiveID);
        this.walActiveFullPath = FileUtil.getDataDirFilePath(walActiveID);
        this.walSilenceFullPath = FileUtil.getDataDirFilePath(walSilenceID);
        try {
            this.writeChannel = new FileOutputStream(walActiveFullPath, true).getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(ByteBuffer byteBuffer) {
        try {
            writeChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(byte[] key, byte[] value) {
        int kl = key.length;
        int vl = value.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + kl + vl);
        byteBuffer.putInt(kl);
        byteBuffer.put(key);
        byteBuffer.putInt(vl);
        byteBuffer.put(value);
        byteBuffer.flip();
        append(byteBuffer);
    }

    public void append(String key, String value) {
        append(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public void close() {
        try {
            writeChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete() {
        log.info("Deleting Silence WAL file {}", walSilenceFullPath);
        boolean deleteFlag = FileUtil.deleteDataDirFile(walSilenceID);
        if(!deleteFlag) {
            throw new RuntimeException("Failed to delete WAL file " + walSilenceFullPath);
        }
    }

    public void changeState() {
        try {
            writeChannel.close();
            log.info("Changing WAL file from {} to {}", walActiveFullPath, walSilenceFullPath);
            boolean flag = FileUtil.renameDataDirFile(walActiveID, walSilenceID);
            if(!flag) {
                throw new RuntimeException("Failed to rename WAL file " + walActiveFullPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getActiveID() {
        return "wal_active_" + IDAssign.getWALNextID() + ".wal";
    }

    private String getSilenceID(String activeID) {
        return activeID.replace("active", "silence");
    }

}
