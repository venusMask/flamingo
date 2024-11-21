package org.apache.flamingo.bean;

import lombok.Getter;

@Getter
public class KLogEntity {

	private long totalSize;

	private boolean deleteFlag;

	private boolean storeMode;

	private int keySize;

	private byte[] key;

	private int valueSize;

	private byte[] value;

	private long fieldID;

	private long offset;

	public KLogEntity(long totalSize, boolean deleteFlag, boolean storeMode, int keySize, byte[] key, int valueSize,
			byte[] value, long fieldID, long offset) {
		this.totalSize = key.length + value.length;
		this.deleteFlag = deleteFlag;
		this.storeMode = storeMode;
		this.keySize = keySize;
		this.key = key;
		this.valueSize = valueSize;
		this.value = value;
		this.fieldID = fieldID;
		this.offset = offset;
	}

}
