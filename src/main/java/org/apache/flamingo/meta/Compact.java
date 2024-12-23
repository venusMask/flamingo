package org.apache.flamingo.meta;

import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.bean.CompactEntity;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.file.NamedUtil;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.utils.Pair;
import org.apache.flamingo.utils.StringUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class Compact {

	private final MetaInfo metaInfo;

	public Compact(MetaInfo metaInfo) {
		this.metaInfo = metaInfo;
	}

	/**
	 * <a href="https://en.wikipedia.org/wiki/Merge_sort">File Merge Sort</a>
	 * @param upperLevelSST New data. Absolutely impossible to be null or empty! When
	 * duplicate keys appear during the merging process, the new data takes effect
	 * @param lowerLevelSST Old data. May be empty, but absolutely impossible to be null!
	 */
	public void majorCompact(List<SSTMetaInfo> upperLevelSST, List<SSTMetaInfo> lowerLevelSST) {
		ArrayList<SSTMetaInfo> newLowerLevelSST = new ArrayList<>();
		logCompactInfo(upperLevelSST, lowerLevelSST);
		final int maxEntriesPerFile = Integer.parseInt(Options.SSTableMaxSize.getValue());
		// The lowerLevelSST may be empty, so the target level cannot be taken from it.
		final int targetLevel = upperLevelSST.get(0).getLevel() + 1;
		List<DataInputStream> newReaders = createReaders(upperLevelSST);
		List<DataInputStream> oldReaders = createReaders(lowerLevelSST);
		// The same key needs to ensure that the data in the upper level SST pops up first
		PriorityQueue<CompactEntity> queue = new PriorityQueue<>((o1, o2) -> {
			int keyComparison = StringUtil.compareByteArrays(o1.getKey(), o2.getKey());
			if (keyComparison != 0) {
				return keyComparison;
			}
			return Boolean.compare(o1.isFromNewSSTable(), o2.isFromNewSSTable());
		});
		loadInitialEntries(queue, newReaders, true);
		loadInitialEntries(queue, oldReaders, false);
		Pair<String, Long> pair = NamedUtil.getKeyFilePath();
		DataOutputStream writer = createNewTargetWriter(pair.getF0());
		long entryCount = 0;
		byte[] lastKey = null;
		assert queue.peek() != null;
		byte[] minKey = queue.peek().getKey();
		byte[] maxKey = queue.peek().getKey();
		try {
			while (!queue.isEmpty()) {
				CompactEntity entry = queue.poll();
				byte[] entryKey = entry.getKey();
				if (StringUtil.compareByteArrays(minKey, entryKey) > 0) {
					minKey = entryKey;
				}
				if (StringUtil.compareByteArrays(maxKey, entryKey) < 0) {
					maxKey = entryKey;
				}
				// There are several situations as follows
				// 1: First write: direct write
				// 2: The key is different from the previous key: direct write
				// 3: The key is same as last time, Because the data that pops up first is
				// of higher priority,
				// the data that pops up at this time is directly discarded.
				if (lastKey == null || !Arrays.equals(lastKey, entryKey)) {
					writer.write(entry.toBytes());
					entryCount++;
					lastKey = entryKey;
				}
				if (entryCount >= maxEntriesPerFile) {
					writer.flush();
					writer.close();
					SSTMetaInfo sst = SSTMetaInfo.builder()
						.fileName(pair.getF0())
						.id(String.valueOf(pair.getF1()))
						.level(targetLevel)
						.minimumValue(minKey)
						.count(entryCount)
						.maximumValue(maxKey)
						.build();
					newLowerLevelSST.add(sst);
					pair = NamedUtil.getKeyFilePath();
					writer = createNewTargetWriter(pair.getF0());
					entryCount = 0;
				}
				if (entry.hasRemaining()) {
					queue.offer(new CompactEntity(entry.getReader(), entry.isFromNewSSTable()));
				}
				else {
					entry.close();
				}
			}
			writer.flush();
			writer.close();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (entryCount > 0) {
			try {
				writer.flush();
				writer.close();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			SSTMetaInfo sst = SSTMetaInfo.builder()
				.fileName(pair.getF0())
				.id(String.valueOf(pair.getF1()))
				.level(targetLevel)
				.minimumValue(minKey)
				.count(entryCount)
				.maximumValue(maxKey)
				.build();
			newLowerLevelSST.add(sst);
		}
		addMetaInfo(newLowerLevelSST);
		// Remove
		removeDeleteInfo(upperLevelSST);
		if (!lowerLevelSST.isEmpty()) {
			removeDeleteInfo(lowerLevelSST);
		}
	}

	/**
	 * Delete the merged file information after the merge is completed
	 */
	private void removeDeleteInfo(List<SSTMetaInfo> needDelSST) {
		metaInfo.removeTable(needDelSST);
	}

	private void addMetaInfo(List<SSTMetaInfo> sstTables) {
		log.info("new files generated by merging: ");
		sstTables.forEach(table -> {
			log.debug(table.toString());
			metaInfo.addTable(table);
		});
	}

	private List<DataInputStream> createReaders(List<SSTMetaInfo> tables) {
		List<DataInputStream> readers = new ArrayList<>();
		for (SSTMetaInfo table : tables) {
			String fileName = table.getFileName();
			FileUtil.checkFileExists(fileName, true);
			InputStream inputStream;
			try {
				inputStream = Files.newInputStream(Paths.get(fileName));
			}
			catch (IOException e) {
				throw new RuntimeException("Read file " + fileName + " failed", e);
			}
			readers.add(new DataInputStream(inputStream));
		}
		return readers;
	}

	private DataOutputStream createNewTargetWriter(String targetFileName) {
		OutputStream outputStream;
		try {
			outputStream = Files.newOutputStream(Paths.get(targetFileName));
		}
		catch (IOException e) {
			throw new RuntimeException("Create file " + targetFileName + " failed", e);
		}
		return new DataOutputStream(new BufferedOutputStream(outputStream));
	}

	private void loadInitialEntries(PriorityQueue<CompactEntity> queue, List<DataInputStream> readers,
			boolean fromNewSSTable) {
		for (DataInputStream reader : readers) {
			queue.offer(new CompactEntity(reader, fromNewSSTable));
		}
	}

	private void logCompactInfo(List<SSTMetaInfo> upperLevelSST, List<SSTMetaInfo> lowerLevelSST) {
		log.debug("Begin Major Compaction, Print Compaction Info");
		log.debug("High priority SST Files ");
		upperLevelSST.forEach(System.out::println);
		log.debug("Low priority SST Files ");
		lowerLevelSST.forEach(System.out::println);
		log.debug("End Print Compaction Info!");
	}

}
