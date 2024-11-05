package org.apache.flamingo.wal;

import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.memtable.DefaultMemTable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class WALWriter {

    private final DefaultMemTable memTable;

    private final FileChannel fileChannel;

    private final String walActiveLogName;

    private final String walDeadLogName;

    public WALWriter(DefaultMemTable memTable) {
        this.memTable = memTable;
        this.walActiveLogName = FileUtil.getWalActiveName();
        this.walDeadLogName = dealName();
        try {
            this.fileChannel = new FileOutputStream(walActiveLogName, true).getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private String dealName() {
        String[] paths = walActiveLogName.split(File.separator);
        int lastIndex = paths.length - 1;
        paths[lastIndex] = paths[lastIndex].replace("active", "dead");
        return String.join(File.separator, paths);
    }

    public void append(ByteBuffer byteBuffer) {
        try {
            fileChannel.write(byteBuffer);
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
            fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete() {
        FileUtil.deleteDataDirFile(walDeadLogName);
    }

    public void changeState() {
        try {
            fileChannel.close();
            FileUtil.renameDataDirFile(walActiveLogName, walDeadLogName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
