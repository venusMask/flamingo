package org.apache.flamingo.sstable;

import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * SSTable
 */
@Getter
public class SSTable {

	public static final String SSTABLE = "sstable_";

	private final String filePath;

	private final int level;

	public SSTable(String filePath, int level) {
		this.filePath = filePath;
		this.level = level;
	}

	public static byte[] serialize(SSTable ssTable) throws IOException {
		int level = ssTable.getLevel();
		String filePath = ssTable.getFilePath();
		ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + filePath.length());
		// Total length
		byteBuffer.putInt(4 + filePath.length());
		// level value
		byteBuffer.putInt(level);
		// file value
		byteBuffer.put(filePath.getBytes(StandardCharsets.UTF_8));
		return byteBuffer.array();
	}

	public static SSTable deserialize(ByteBuffer byteBuffer) throws IOException {
		int totalSize = byteBuffer.getInt();
		int level = byteBuffer.getInt();
		byte[] fileByte = new byte[totalSize - 4];
		byteBuffer.get(fileByte);
		String filePath = new String(fileByte, StandardCharsets.UTF_8);
		return new SSTable(filePath, level);
	}

	@Override
	public String toString() {
		return "SSTable{" + "filePath='" + filePath + '\'' + ", level=" + level + '}';
	}

}
