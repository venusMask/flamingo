package org.apache.flamingo.writer;

import lombok.Getter;
import org.apache.flamingo.bean.VLogAddress;
import org.apache.flamingo.bean.VLogEntity;
import org.apache.flamingo.file.NamedUtil;
import org.apache.flamingo.utils.Pair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 往Value_log中写入的对象
 */
@Getter
public class VLogWriter {

    public static final String ACTIVE = "v_active_%d.wal";

    public static final String SILENCE = "v_silence_%d.wal";

    private final RandomAccessFile writer;

    private final long activeID;

    private final String activeFullPath;

    public VLogWriter() {
        Pair<String, Long> pair = NamedUtil.getValueFilePath();
        this.activeID = pair.getF1();
        this.activeFullPath = pair.getF0();
        try {
            this.writer = new RandomAccessFile(activeFullPath, "rw");
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

//    public VLogAddress append(byte[] key, byte[] value, boolean deleted) throws IOException {
//        VLogEntity entity = VLogEntity.from(key, value, deleted);
//        return write(entity);
//    }

    public VLogAddress write(VLogEntity entity) throws IOException {
        long offset = writer.getFilePointer();
        byte[] serialize = VLogEntity.serialize(entity);
        writer.write(serialize);
        return VLogAddress.from(activeID, offset);
    }

    public void close() {
        try {
            writer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
