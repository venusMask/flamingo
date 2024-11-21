package org.apache.flamingo.bean;

import lombok.Getter;
import org.apache.flamingo.utils.StringUtil;

import java.nio.ByteBuffer;

@Getter
public class VLogEntity {

    private final byte[] key;

    private final byte[] value;

    private final boolean deleted;

    private final int totalSize;

    public VLogEntity(byte[] key, byte[] value, boolean deleted) {
        this.key = key;
        this.value = value;
        this.deleted = deleted;
        this.totalSize = getTotalSize();
    }

    /**
     * [total_size,delete_flag,key_size,key,value_size,value]
     * @return total size
     */
    public int getTotalSize() {
        int total = 0;
        total += 4;
        total += 1;
        total += 4;
        total += key.length;
        if(!deleted) {
            total += 4;
            total += value.length;
        }
        return total;
    }

    public static byte[] serialize(VLogEntity entity) {
        ByteBuffer buffer = ByteBuffer.allocate(entity.getTotalSize());
        buffer.putInt(entity.getTotalSize());
        buffer.put(StringUtil.fromBool(entity.isDeleted()));
        buffer.putInt(entity.getKey().length);
        buffer.put(entity.getKey());
        if(!entity.isDeleted()) {
            buffer.putInt(entity.getValue().length);
            buffer.put(entity.getValue());
        }
        return buffer.array();
    }

    public static VLogEntity from(byte[] key, byte[] value, boolean deleted) {
        return new VLogEntity(key, value, deleted);
    }

}

