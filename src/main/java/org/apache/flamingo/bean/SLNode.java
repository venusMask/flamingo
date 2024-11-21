package org.apache.flamingo.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.flamingo.utils.StringUtil;

import java.nio.ByteBuffer;

/**
 * Skip List Node
 * [total_size, delete_flag, store_mode(true), key_size, key_value, file_id, offset]
 * [total_size, delete_flag, store_mode(false), key_size, key_value, value_size, value]
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
public class SLNode {

    public static final byte[] HeadKey = StringUtil.fromString("HEAD");

    public static final byte[] TailKey = StringUtil.fromString("TAIL");

    private byte[] key;

    private byte[] value;

	/**
	 * 存储模式
	 * true  : kv 分离存储
	 * false : kv 一起存储
	 */
	private boolean storeMode = false;

	/**
	 * 在kv分离模式中, 存储了地址
	 */
	private VLogAddress address;

    private boolean deleted = false;

    private SLNode right;

    private SLNode left;

    private SLNode up;

    private SLNode down;

    public SLNode(byte[] key, byte[] value) {
        this(key, value, false);
    }

    public SLNode(byte[] key, byte[] value, boolean deleted) {
        this.key = key;
        this.value = value;
        this.deleted = deleted;
    }

    public SLNode() {
    }

    public SLNode(byte[] key,
                  byte[] value,
                  boolean storeMode,
                  VLogAddress address,
                  boolean deleted) {
        this.key = key;
        this.value = value;
        this.storeMode = storeMode;
        this.address = address;
        this.deleted = deleted;
    }

    public static byte[] serialize(SLNode node) {
        int total = getTotal(node);
        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.putInt(total);
        buffer.put(StringUtil.fromBool(node.isDeleted()));
        buffer.put(StringUtil.fromBool(node.isStoreMode()));
        buffer.putInt(node.getKey().length);
        buffer.put(node.getKey());
        if(node.isStoreMode()) {
            buffer.putLong(node.getAddress().getFieldID());
            buffer.putLong(node.getAddress().getOffset());
        } else {
            buffer.putInt(node.getValue().length);
            buffer.put(node.getValue());
        }
        return buffer.array();
    }

    public static SLNode deserialization(byte[] array) {
        SLNode node = new SLNode();
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        int totalSize = byteBuffer.getInt();
        byte deleteByte = byteBuffer.get();
        boolean deleteFlag = StringUtil.fromByte(deleteByte);
        byte storeModeByte = byteBuffer.get();
        boolean storeModeFlag = StringUtil.fromByte(storeModeByte);
        int keySize = byteBuffer.getInt();
        byte[] key = new byte[keySize];
        byteBuffer.get(key);
        if(storeModeFlag) {
            long fieldID = byteBuffer.getLong();
            long offset = byteBuffer.getLong();
            node.setAddress(VLogAddress.from(fieldID, offset));
        } else {
            int valueSize = byteBuffer.getInt();
            byte[] value = new byte[valueSize];
            byteBuffer.get(value);
            node.setValue(value);
        }
        node.setKey(key);
        node.setStoreMode(storeModeFlag);
        node.setDeleted(deleteFlag);
        return node;
    }

    private static int getTotal(SLNode node) {
        int total = 4;                  // total_size
        total += 1;                     // delete_flag
        total += 1;                     // store_mode
        total += 4;                     // key_size
        byte[] key = node.getKey();
        total += key.length;            //  key value size
        if(node.isStoreMode()) {        // kv store
            total += 8;                 // file size
            total += 8;                 // offset
        } else {
            total += 4;                 // value_size
            byte[] value = node.getValue();
            total += value.length;      // value value size
        }
        return total;
    }

}
