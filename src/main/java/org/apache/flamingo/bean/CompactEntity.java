package org.apache.flamingo.bean;

import lombok.Getter;
import org.apache.flamingo.utils.StringUtil;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

@Getter
public class CompactEntity {

    private final boolean deleted;

    private final boolean storeMode;

    private final byte[] key;

    private byte[] value;

    private VLogAddress address;

    private final boolean fromNewSSTable;

    private final DataInputStream reader;

    public CompactEntity(DataInputStream reader, boolean fromNewSSTable) {
        this.reader = reader;
        this.fromNewSSTable = fromNewSSTable;
        try {
            if (reader != null && reader.available() > 0) {
                int totalSize = reader.readInt();
                byte deleteByte = reader.readByte();
                this.deleted = StringUtil.fromByte(deleteByte);
                byte storeModeByte = reader.readByte();
                this.storeMode = StringUtil.fromByte(storeModeByte);
                int keySize = reader.readInt();
                byte[] key = new byte[keySize];
                reader.readFully(key);
                this.key = key;
                if(storeMode) {
                    long fieldID = reader.readLong();
                    long offset = reader.readLong();
                    this.address = VLogAddress.from(fieldID, offset);
                } else {
                    int valueSize = reader.readInt();
                    byte[] value = new byte[valueSize];
                    reader.readFully(value);
                    this.value = value;
                }
            } else {
                this.key = null;
                this.deleted = false;
                this.storeMode = false;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasRemaining() {
        try {
            return reader != null && reader.available() > 0;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] toBytes() {
        int total = getTotal();
        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.putInt(total);
        buffer.put(StringUtil.fromBool(deleted));
        buffer.put(StringUtil.fromBool(storeMode));
        buffer.putInt(key.length);
        buffer.put(key);
        if(storeMode) {
            buffer.putLong(address.getFieldID());
            buffer.putLong(address.getOffset());
        } else {
            buffer.putInt(value.length);
            buffer.put(value);
        }
        return buffer.array();
    }

    private int getTotal() {
        int total = 4;                  // total_size
        total += 1;                     // delete_flag
        total += 1;                     // store_mode
        total += 4;                     // key_size
        total += key.length;            //  key value size
        if(storeMode) {        // kv store
            total += 8;                 // file size
            total += 8;                 // offset
        } else {
            total += 4;                 // value_size
            total += value.length;      // value value size
        }
        return total;
    }

    public void close() {
        if (reader != null) {
            try {
                reader.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
