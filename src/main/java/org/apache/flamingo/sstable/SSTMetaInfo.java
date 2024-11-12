package org.apache.flamingo.sstable;

import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.utils.StringUtil;
import org.apache.logging.log4j.core.util.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Store SSTable meta information.
 */
@Slf4j
public class SSTMetaInfo {

	/**
	 * Key: level number eg, 0,1,2....
	 */
	private final Map<Integer, List<SSTableInfo>> metaInfo = new HashMap<>();

	private int maxSize = 2;

	private final String metaFilePath;

	public SSTMetaInfo() {
		this.metaFilePath = FileUtil.getMetaInfoPath();
		deserialize();
	}

	public void addLevelTable(SSTableInfo table, int level) {
		metaInfo.computeIfAbsent(level, k -> new ArrayList<>()).add(table);
		serialize();
		List<SSTableInfo> tables = metaInfo.get(level);
		if (tables.size() > maxSize) {
			log.debug("Table size {}, Beginning compact", tables.size());
			compact(level);
		}
	}

	public void addFirstLevel(SSTableInfo ssTable) {
		addLevelTable(ssTable, 0);
	}

	/**
	 * 1: 从需要merge的层ln中挑选出需要合并的文件 2: 从l_n+1层中挑选出范围有重合的文件, 3: 执行合并
	 * @param level
	 */
	public void compact(int level) {
		List<SSTableInfo> newTables = pickNewTables(level);
		ArrayList<SSTableInfo> oldTables = new ArrayList<>();
		int nextLevel = level + 1;
		List<SSTableInfo> oldLevel = metaInfo.get(nextLevel);
		newTables.forEach(newTable -> {
			oldLevel.forEach(oldTable -> {
				if (hasOverlap(newTable, oldTable)) {
					oldTables.add(oldTable);
				}
			});
		});
		Compact compact = new Compact();
		compact.levelCompact(newTables, oldTables);
	}

	/**
	 * 挑选需要合并的文件
	 */
	private List<SSTableInfo> pickNewTables(int level) {
		return Collections.singletonList(metaInfo.get(level).get(0));
	}

	private boolean hasOverlap(SSTableInfo newTable, SSTableInfo oldTable) {
		byte[] newMin = newTable.getMetaInfo().getMinimumValue();
		byte[] oldMin = oldTable.getMetaInfo().getMinimumValue();
		byte[] newMax = newTable.getMetaInfo().getMaximumValue();
		byte[] oldMax = oldTable.getMetaInfo().getMaximumValue();
		return StringUtil.compareByteArrays(newMax, oldMin) >= 0 && StringUtil.compareByteArrays(oldMax, newMin) >= 0;
	}

	private void serialize() {
		log.debug("serialize meta information to disk!");
		try (OutputStream outputStream = Files.newOutputStream(Paths.get(metaFilePath));
				BufferedOutputStream writer = new BufferedOutputStream(outputStream)) {
			metaInfo.values().forEach(list -> list.forEach(table -> {
				try {
					byte[] serialized = SSTableInfo.serialize(table);
					byte[] array = ByteBuffer.allocate(4 + serialized.length)
						.putInt(serialized.length)
						.put(serialized)
						.array();
					writer.write(array);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}));
			writer.flush();
		}
		catch (Exception exception) {
			throw new RuntimeException(exception);
		}
	}

	public void deserialize() {
		log.debug("deserialize meta information from disk!");
		if (FileUtil.checkFileExists(metaFilePath, false)) {
			try (InputStream inputStream = Files.newInputStream(Paths.get(metaFilePath));
					BufferedInputStream reader = new BufferedInputStream(inputStream)) {
				while (reader.available() > 0) {
					byte[] lengthBytes = new byte[4];
					if (reader.read(lengthBytes) != 4)
						break;
					int length = ByteBuffer.wrap(lengthBytes).getInt();
					byte[] serialized = new byte[length];
					if (reader.read(serialized) != length)
						break;
					SSTableInfo sst = SSTableInfo.deserialize(serialized);
					int level = sst.getLevel();
					metaInfo.computeIfAbsent(level, k -> new ArrayList<>()).add(sst);
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Failed to deserialize meta information", e);
			}
		}
	}

	public List<SSTableInfo> getLevel(int level) {
		List<SSTableInfo> list = metaInfo.get(level);
		if (list == null) {
			throw new RuntimeException("Level " + level + " not found");
		}
		return list;
	}

}
