package org.apache.flamingo.sstable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.utils.Pair;
import org.apache.flamingo.utils.StringUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store sst meta information.
 */
@Slf4j
public class SSTMetaInfo {

	@Getter
	private final Map<Integer, List<SSTableInfo>> metaInfo = new ConcurrentHashMap<>();

	private int maxSize = 2;

	private final String metaFilePath;

	public SSTMetaInfo() {
		this(FileUtil.getMetaInfoPath());
	}

	public SSTMetaInfo(String metaFilePath) {
		this.metaFilePath = metaFilePath;
		deserialize();
	}

	public void addLevelTable(SSTableInfo table, int level) {
		metaInfo.computeIfAbsent(level, k -> Collections.synchronizedList(new ArrayList<>())).add(table);
		serialize();
		List<SSTableInfo> tables = metaInfo.get(level);
		if (tables.size() > maxSize) {
			log.debug("level: {}, sst size {}, Compact start...", level, tables.size());
			compact(level);
		}
	}

	public void addFirstLevel(SSTableInfo ssTable) {
		addLevelTable(ssTable, 0);
	}

	public void compact(int level) {
		Compact compact = new Compact();
		if (level == 0) {
			List<SSTableInfo> zeroTables = new ArrayList<>(metaInfo.get(0));
			zeroTables.forEach(zeroTable -> {
				List<SSTableInfo> newTables = Collections.singletonList(zeroTable);
				List<SSTableInfo> needCompactSST = hasOverlap(newTables, 1);
				compact.majorCompact(newTables, needCompactSST);
			});
		}
		else {
			List<SSTableInfo> newTables = pickNewTables(level);
			List<SSTableInfo> needCompactSST = hasOverlap(newTables, level + 1);
			compact.majorCompact(newTables, needCompactSST);
		}
	}

	public List<SSTableInfo> hasOverlap(List<SSTableInfo> newTables, int nextLevel) {
		ArrayList<SSTableInfo> needCompactSST = new ArrayList<>();
		List<SSTableInfo> oldTables = metaInfo.getOrDefault(nextLevel, Collections.emptyList());
		newTables.forEach(newTable -> oldTables.forEach(oldTable -> {
			if (hasOverlap(newTable, oldTable)) {
				needCompactSST.add(oldTable);
			}
		}));
		return needCompactSST;
	}

	/**
	 * 挑选需要合并的文件
	 */
	private List<SSTableInfo> pickNewTables(int level) {
		return Collections.singletonList(metaInfo.get(level).get(0));
	}

	private boolean hasOverlap(SSTableInfo newTable, SSTableInfo oldTable) {
		byte[] newMin = newTable.getMinimumValue();
		byte[] oldMin = oldTable.getMinimumValue();
		byte[] newMax = newTable.getMaximumValue();
		byte[] oldMax = oldTable.getMaximumValue();
		return StringUtil.compareByteArrays(newMax, oldMin) >= 0 && StringUtil.compareByteArrays(oldMax, newMin) >= 0;
	}

	public void serialize() {
		log.debug("sst meta change, serialize meta information to disk!");
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
					metaInfo.computeIfAbsent(level, k -> Collections.synchronizedList(new ArrayList<>())).add(sst);
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Failed to deserialize meta information", e);
			}
		}
		else {
			log.debug("Meta information file does not exist, skipping deserialization!");
		}
	}

	public List<SSTableInfo> getLevel(int level) {
		List<SSTableInfo> list = metaInfo.get(level);
		if (list == null) {
			throw new RuntimeException("Level " + level + " not found");
		}
		return list;
	}

	private Pair<byte[], Boolean> searchFromZeroLevel(byte[] key) {
		List<SSTableInfo> tables = metaInfo.get(0);
		if(tables == null) {
			return Pair.of(null, false);
		}
		for (SSTableInfo table : tables) {
			Pair<byte[], Boolean> pair = table.search(key);
			if (pair.getF1()) {
				return pair;
			}
		}
		return Pair.of(null, false);
	}

	public byte[] search(byte[] key) {
		Pair<byte[], Boolean> pair = searchFromZeroLevel(key);
		if (pair.getF1()) {
			return pair.getF0();
		}
		Set<Integer> keySet = metaInfo.keySet();
		keySet.remove(0);
		ArrayList<Integer> keys = new ArrayList<>(keySet);
		Collections.sort(keys);
		for (Integer levelNumber : keys) {
			List<SSTableInfo> level = metaInfo.get(levelNumber);
			for (int j = level.size() - 1; j >= 0; j--) {
				SSTableInfo mayBeSearchSST = level.get(j);
				byte[] minimumValue = mayBeSearchSST.getMinimumValue();
				byte[] maximumValue = mayBeSearchSST.getMaximumValue();
				if (StringUtil.compareByteArrays(minimumValue, key) <= 0
						&& StringUtil.compareByteArrays(maximumValue, key) >= 0) {
					Pair<byte[], Boolean> searchPair = mayBeSearchSST.search(key);
					if (searchPair.getF1()) {
						return searchPair.getF0();
					}
					break;
				}
			}
		}
		return null;
	}

}
