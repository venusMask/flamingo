package org.apache.flamingo.sstable;

import lombok.Getter;
import org.apache.flamingo.file.FileUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * SSTable
 */
@Getter
public class SSTableInfo {

	public static final String SSTABLE = "sstable_";

	private final String fileName;

	private final int level;

	public SSTableInfo(String filePath, int level) {
		this.fileName = filePath;
		this.level = level;
	}

	public static byte[] serialize(SSTableInfo ssTable) throws IOException {
		int level = ssTable.getLevel();
		String filePath = ssTable.getFileName();
		ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + filePath.length());
		// Total length
		byteBuffer.putInt(4 + filePath.length());
		// level value
		byteBuffer.putInt(level);
		// file value
		byteBuffer.put(filePath.getBytes(StandardCharsets.UTF_8));
		return byteBuffer.array();
	}

	public static SSTableInfo deserialize(ByteBuffer byteBuffer) throws IOException {
		int totalSize = byteBuffer.getInt();
		int level = byteBuffer.getInt();
		byte[] fileByte = new byte[totalSize - 4];
		byteBuffer.get(fileByte);
		String filePath = new String(fileByte, StandardCharsets.UTF_8);
		return new SSTableInfo(filePath, level);
	}

	@Override
	public String toString() {
		return "SSTable{" + "filePath='" + fileName + '\'' + ", level=" + level + '}';
	}

	public static SSTableInfo create(int level) {
		String fileName = FileUtil.getSSTFileName();
		return new SSTableInfo(fileName, level);
	}

}
