package org.apache.flamingo.sstable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import org.apache.flamingo.file.FileUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

	@Data
	@Builder
	@AllArgsConstructor
	public static class MetaInfo {

		private byte[] minimumValue;

		private byte[] maximumValue;

		private long createTime;

		private long lastUseTime;

		public MetaInfo() {
		}

		public static byte[] serialize(MetaInfo metaInfo) throws JsonProcessingException {
			return ObjectMapper.writeValueAsBytes(metaInfo);
		}

		public static MetaInfo deserialize(byte[] byteArray) throws IOException {
			return ObjectMapper.readValue(byteArray, MetaInfo.class);
		}

	}

}
