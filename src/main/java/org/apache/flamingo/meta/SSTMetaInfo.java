package org.apache.flamingo.meta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.core.Context;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.utils.Pair;
import org.apache.flamingo.utils.StringUtil;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

@Slf4j
@Data
@Builder
@AllArgsConstructor
public class SSTMetaInfo {

	public static final String SSTABLE = "sstable_";

	private static final ObjectMapper ObjectMapper = Context.getInstance().getObjectMapper();

	//////////////////////////////////////
	//
	// Meta Info About SST
	//
	//////////////////////////////////////

	private String fileName;

	private int level;

	private String id;

	private byte[] minimumValue;

	private byte[] maximumValue;

	private long count = 0;

	private long createTime = System.currentTimeMillis();

	public SSTMetaInfo() {}

	public void delete() {
		FileUtil.deleteFile(fileName);
	}

	public static JsonNode toJson(SSTMetaInfo tableInfo) throws IOException {
		if(tableInfo != null) {
			return ObjectMapper.valueToTree(tableInfo);
		}
		return null;
	}

	public static SSTMetaInfo fromJSON(JsonNode node) {
		String fileName = node.get("fileName").asText();
		int level = node.get("level").asInt();
		String id = node.get("id").asText();
		String minimumValue = node.get("minimumValue").asText();
		String maximumValue = node.get("maximumValue").asText();
		long count = node.get("count").asLong();
		long createTime = node.get("createTime").asLong();
		return SSTMetaInfo.builder()
				.fileName(fileName)
				.level(level)
				.id(id)
				.minimumValue(StringUtil.fromString(minimumValue))
				.maximumValue(StringUtil.fromString(maximumValue))
				.count(count)
				.createTime(createTime)
				.build();
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
}
