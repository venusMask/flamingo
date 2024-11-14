package org.apache.flamingo.sstable;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.core.LSMContext;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.utils.StringUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class Compact {

	private final LSMContext context = LSMContext.getInstance();

	private final SSTMetaInfo metaInfo = context.getSstMetadata();

	/**
	 * <a href="https://en.wikipedia.org/wiki/Merge_sort">File Merge Sort</a>
	 * @param upperLevelSST New data. Absolutely impossible to be null or empty! When
	 * duplicate keys appear during the merging process, the new data takes effect
	 * @param lowerLevelSST Old data. May be empty, but absolutely impossible to be null!
	 */
	public void majorCompact(List<SSTableInfo> upperLevelSST, List<SSTableInfo> lowerLevelSST) {
		ArrayList<SSTableInfo> newLowerLevelSST = new ArrayList<>();
		logCompactInfo(upperLevelSST, lowerLevelSST);
		final int maxEntriesPerFile = Integer.parseInt(Options.SSTableMaxSize.getValue());
		final int targetLevel = upperLevelSST.get(0).getLevel() + 1;
		List<DataInputStream> newReaders = createReaders(upperLevelSST);
		List<DataInputStream> oldReaders = createReaders(lowerLevelSST);
		// The same key needs to ensure that the data in the upper level SST pops up first
		PriorityQueue<Entry> queue = new PriorityQueue<>((o1, o2) -> {
			int keyComparison = StringUtil.compareByteArrays(o1.getKey(), o2.getKey());
			if (keyComparison != 0) {
				return keyComparison;
			}
			return Boolean.compare(o1.fromNewSSTable, o2.fromNewSSTable);
		});
		loadInitialEntries(queue, newReaders, true);
		loadInitialEntries(queue, oldReaders, false);
		String targetFileName = FileUtil.getSSTFileName();
		DataOutputStream writer = createNewTargetWriter(targetFileName);
		long entryCount = 0;
		byte[] lastKey = null;
		assert queue.peek() != null;
		byte[] minKey = queue.peek().getKey();
		byte[] maxKey = queue.peek().getKey();
		try {
			while (!queue.isEmpty()) {
				Entry entry = queue.poll();
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
					SSTableInfo sst = SSTableInfo.builder()
						.fileName(targetFileName)
						.level(targetLevel)
						.metaInfo(SSTableInfo.MetaInfo.create(minKey, maxKey, entryCount))
						.build();
					newLowerLevelSST.add(sst);
					targetFileName = FileUtil.getSSTFileName();
					writer = createNewTargetWriter(targetFileName);
					entryCount = 0;
				}
				if (entry.hasRemaining()) {
					queue.offer(new Entry(entry.reader, entry.fromNewSSTable));
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
			SSTableInfo sst = SSTableInfo.builder()
				.fileName(targetFileName)
				.level(targetLevel)
				.metaInfo(SSTableInfo.MetaInfo.create(minKey, maxKey, entryCount))
				.build();
			newLowerLevelSST.add(sst);
		}
		addMetaInfo(newLowerLevelSST, targetLevel);
		removeDeleteInfo(upperLevelSST);
		if (!lowerLevelSST.isEmpty()) {
			removeDeleteInfo(lowerLevelSST);
		}
	}

	/**
	 * Delete the merged file information after the merge is completed
	 */
	private void removeDeleteInfo(List<SSTableInfo> needDelSST) {
		ArrayList<SSTableInfo> copyTables = new ArrayList<>(needDelSST);
		int level = needDelSST.get(0).getLevel();
		try {
			log.debug("Before delete meta Info: {}",
					new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(metaInfo));
		}
		catch (IOException ignore) {
		}
		List<SSTableInfo> collections = metaInfo.getLevel(level);
		collections.removeAll(needDelSST);
		if (collections.isEmpty()) {
			log.debug("level {} is empty, remove current level.", level);
			metaInfo.getMetaInfo().remove(level);
		}
		copyTables.forEach(SSTableInfo::delete);
		try {
			log.debug("After delete meta Info: {}",
					new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(metaInfo));
		}
		catch (IOException ignore) {
		}
		metaInfo.serialize();
	}

	private void addMetaInfo(List<SSTableInfo> sstTables, int level) {
		log.info("new files generated by merging: ");
		sstTables.forEach(table -> {
			log.debug(table.toString());
			metaInfo.addLevelTable(table, level);
		});
	}

	private List<DataInputStream> createReaders(List<SSTableInfo> tables) {
		List<DataInputStream> readers = new ArrayList<>();
		for (SSTableInfo table : tables) {
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

	private void loadInitialEntries(PriorityQueue<Entry> queue, List<DataInputStream> readers, boolean fromNewSSTable) {
		for (DataInputStream reader : readers) {
			queue.offer(new Entry(reader, fromNewSSTable));
		}
	}

	private void logCompactInfo(List<SSTableInfo> upperLevelSST, List<SSTableInfo> lowerLevelSST) {
		log.debug("  Begin Major Compaction, Print Compaction Info  ");
		log.debug("  High priority SST Files ");
		upperLevelSST.forEach(System.out::println);
		log.debug("  Low priority SST Files ");
		lowerLevelSST.forEach(System.out::println);
		log.debug("  End Major Compaction, Success Compaction ! ");
	}

	@Getter
	public static class Entry {

		private final byte[] key;

		private final byte[] value;

		private final Boolean isDeleted;

		private final boolean fromNewSSTable;

		private final DataInputStream reader;

		public Entry(DataInputStream reader, boolean fromNewSSTable) {
			this.reader = reader;
			this.fromNewSSTable = fromNewSSTable;
			try {
				if (reader != null && reader.available() > 0) {
					byte b = reader.readByte();
					this.isDeleted = b == (byte) 1;
					int kl = reader.readInt();
					this.key = new byte[kl];
					reader.readFully(this.key);
					int vl = reader.readInt();
					this.value = new byte[vl];
					reader.readFully(this.value);
				}
				else {
					this.key = null;
					this.value = null;
					this.isDeleted = false;
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public boolean hasRemaining() {
			try {
				return reader != null && reader.available() > 0;
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public byte[] toBytes() {
			int kl = key.length;
			int vl = value.length;
			byte delByte = isDeleted ? (byte) 1 : (byte) 0;
			ByteBuffer byteBuffer = ByteBuffer.allocate(1 + 4 + 4 + kl + vl);
			byteBuffer.put(delByte);
			byteBuffer.putInt(kl);
			byteBuffer.put(key);
			byteBuffer.putInt(vl);
			byteBuffer.put(value);
			return byteBuffer.array();
		}

		public void close() {
			if (reader != null) {
				try {
					reader.close();
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

	}

}
