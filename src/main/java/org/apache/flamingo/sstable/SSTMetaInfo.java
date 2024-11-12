package org.apache.flamingo.sstable;

import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.file.FileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

/**
 * Store SSTable MetaInfo.
 */
@Slf4j
public class SSTMetaInfo {

	private final Map<Integer, List<SSTableInfo>> metaData = new HashMap<>();

	private final String metaFilePath;

	public SSTMetaInfo() {
		this.metaFilePath = FileUtil.getMetaInfoPath();
		init();
	}

	public void addLevelTable(SSTableInfo table, int level) {
		metaData.computeIfAbsent(level, k -> new ArrayList<>()).add(table);
		flushMetadata();
	}

	public void addFirstLevel(SSTableInfo ssTable) {
		addLevelTable(ssTable, 0);
	}

	private void flushMetadata() {
		log.debug("Flushing metadata");
		try {
			FileOutputStream outputStream = new FileOutputStream(metaFilePath);
			metaData.values().forEach(list -> list.forEach(ssTable -> {
				try {
					outputStream.write(SSTableInfo.serialize(ssTable));
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}));
			outputStream.close();
		}
		catch (Exception exception) {
			throw new RuntimeException(exception);
		}
	}

	private void init() {
		File metaInfoFile = new File(metaFilePath);
		try {
			if (metaInfoFile.exists()) {
				try (BufferedInputStream inputStream = new BufferedInputStream(
						Files.newInputStream(metaInfoFile.toPath()))) {
					byte[] fileBytes = new byte[(int) metaInfoFile.length()];
					int read = inputStream.read(fileBytes);
					if (read > 0) {
						ByteBuffer byteBuffer = ByteBuffer.wrap(fileBytes);
						while (byteBuffer.hasRemaining()) {
							SSTableInfo ssTable = SSTableInfo.deserialize(byteBuffer);
							int level = ssTable.getLevel();
							metaData.computeIfAbsent(level, k -> new ArrayList<>()).add(ssTable);
						}
					}
				}
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Error reading meta info file: " + metaFilePath, e);
		}
	}

	public List<SSTableInfo> getLevel(int level) {
		List<SSTableInfo> list = metaData.get(level);
		if (list == null) {
			throw new RuntimeException("Level " + level + " not found");
		}
		return list;
	}

}
