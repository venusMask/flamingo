package org.apache.flamingo.sstable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.utils.Pair;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

@Slf4j
@Data
@Builder
@AllArgsConstructor
public class SSTableInfo {

	public static final String SSTABLE = "sstable_";

	public static final ObjectMapper ObjectMapper = new ObjectMapper();

	private String fileName;

	private int level;

	private MetaInfo metaInfo;

	public SSTableInfo() {
	}

	public SSTableInfo(String filePath, int level) {
		this.fileName = filePath;
		this.level = level;
	}

	public static byte[] serialize(SSTableInfo ssTable) throws IOException {
		return ObjectMapper.writeValueAsBytes(ssTable);
	}

	public static SSTableInfo deserialize(byte[] bytes) throws IOException {
		return ObjectMapper.readValue(bytes, SSTableInfo.class);
	}

	public static SSTableInfo create(int level) {
		String fileName = FileUtil.getSSTFileName();
		return new SSTableInfo(fileName, level);
	}

	public void delete() {
		FileUtil.deleteFile(fileName);
	}

	@Override
	public String toString() {
		return "SSTableInfo [fileName=" + fileName + ", level=" + level + "]";
	}

	/**
	 * @param key search key
	 * @return f0: value, f1: find
	 */
	public Pair<byte[], Boolean> search(byte[] key) {
		try (InputStream inputStream = Files.newInputStream(Paths.get(fileName));
				DataInputStream reader = new DataInputStream(inputStream)) {
			while (reader.available() > 0) {
				byte b = reader.readByte();
				boolean delFlag = b == (byte) 1;
				int kl = reader.readInt();
				byte[] k = new byte[kl];
				reader.readFully(k);
				int vl = reader.readInt();
				byte[] v = new byte[vl];
				reader.readFully(v);
				// Search
				if (Arrays.equals(key, k)) {
					if (delFlag) {
						return Pair.of(null, true);
					}
					else {
						return Pair.of(v, true);
					}
				}
			}
			return Pair.of(null, false);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Data
	@Builder
	@AllArgsConstructor
	public static class MetaInfo {

		private byte[] minimumValue;

		private byte[] maximumValue;

		private long count = 0;

		private long createTime = System.currentTimeMillis();

		private long lastUseTime = System.currentTimeMillis();

		public MetaInfo() {
		}

		public static byte[] serialize(MetaInfo metaInfo) throws JsonProcessingException {
			return ObjectMapper.writeValueAsBytes(metaInfo);
		}

		public static MetaInfo deserialize(byte[] byteArray) throws IOException {
			return ObjectMapper.readValue(byteArray, MetaInfo.class);
		}

		public static MetaInfo create(byte[] min, byte[] max) {
			return create(min, max, 0);
		}

		public static MetaInfo create(byte[] min, byte[] max, long count) {
			return SSTableInfo.MetaInfo.builder()
				.minimumValue(min)
				.maximumValue(max)
				.createTime(System.currentTimeMillis())
				.lastUseTime(System.currentTimeMillis())
				.count(count)
				.build();
		}

	}

}
