package org.apache.flamingo.sstable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.core.LSMContext;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.memtable.skiplist.SkipList;
import org.apache.flamingo.memtable.skiplist.SkipListOption;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.utils.StringUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * 实现SSTable的合并
 *
 * @Author venus
 * @Date 2024/11/11
 * @Version 1.0
 */
@Slf4j
public class Compact {

	private static final int MAX_ENTRIES_PER_FILE = Integer.parseInt(Options.SSTableMaxSize.getValue());

	private final LSMContext context = LSMContext.getInstance();

	private final SSTMetaInfo metaInfo = context.getSstMetadata();

	/**
	 * 合并SSTable 0层的数据 针对第0层的文件,按照写入的先后顺序遍历所有被持久化的文件(后被写入的文件总是具有更高的时效性可以覆盖先被写入的文件)
	 * 在内存中合并所有的sst, 具体的行为是按照顺序将所有的数据插入SkipList中然后再将数据写入第1层的磁盘文件
	 */
	public void compactSSTableZero() {
		SkipListOption option = SkipListOption.builder().maxLevel(32).probability(0.5).build();
		List<SSTableInfo> firstLevel = metaInfo.getLevel(0);
		SkipList skipList = new SkipList();
		for (SSTableInfo ssTable : firstLevel) {
			SkipList buildSkipList = SkipList.build(option, ssTable);
			skipList.merge(buildSkipList);
		}
		SSTableInfo firstLevelTable = SSTableInfo.create(1);
		skipList.flush(firstLevelTable);
		metaInfo.addLevelTable(firstLevelTable, 1);
	}

	/**
	 * <a href="https://en.wikipedia.org/wiki/Merge_sort">归并排序</a>
	 * <p>
	 * Use merge sort to merge the data in newSSTable and oldSSTable. Note that the data
	 * in newSSTable and oldSSTable is already sorted!!! If there are duplicate keys, use
	 * the one in newSSTable to overwrite the one in oldSSTable
	 */
	public void levelCompact(List<SSTableInfo> newSSTables, List<SSTableInfo> oldSSTables) {
		int targetLevel = newSSTables.get(0).getLevel();
		List<DataInputStream> newReaders = createReaders(newSSTables);
		List<DataInputStream> oldReaders = createReaders(oldSSTables);
		PriorityQueue<Entry> queue = new PriorityQueue<>(
				(o1, o2) -> StringUtil.compareByteArrays(o1.getKey(), o2.getKey()));
		// Load from newSSTables first (higher priority)
		loadInitialEntries(queue, newReaders, true);
		loadInitialEntries(queue, oldReaders, false);
		String targetFileName = FileUtil.getSSTFileName();
		DataOutputStream writer = createNewTargetWriter(targetFileName);
		int entryCount = 0;
		byte[] lastKey = null;
		try {
			while (!queue.isEmpty()) {
				Entry entry = queue.poll();
				if (lastKey == null || !Arrays.equals(lastKey, entry.key)) {
					writer.write(entry.toBytes());
					entryCount++;
					lastKey = entry.key;
					if (entryCount >= MAX_ENTRIES_PER_FILE) {
						writer.close();
						addMetaInfo(targetFileName, targetLevel);
						targetFileName = FileUtil.getSSTFileName();
						writer = createNewTargetWriter(targetFileName);
						entryCount = 0;
					}
				}
				if (entry.hasRemaining()) {
					queue.offer(new Entry(entry.reader, entry.fromNewSSTable));
				}
				else {
					entry.close();
				}
			}
			writer.close();
			addMetaInfo(targetFileName, targetLevel);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void addMetaInfo(String fileName, int level) {
		SSTableInfo ssTableInfo = new SSTableInfo(fileName, level);
		metaInfo.addLevelTable(ssTableInfo, level);
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

	@Getter
	public static class Entry {

		private final byte[] key;

		private final byte[] value;

		private final boolean fromNewSSTable;

		private final DataInputStream reader;

		public Entry(DataInputStream reader, boolean fromNewSSTable) {
			this.reader = reader;
			this.fromNewSSTable = fromNewSSTable;
			try {
				int kl = reader.readInt();
				this.key = new byte[kl];
				reader.readFully(this.key);
				int vl = reader.readInt();
				this.value = new byte[vl];
				reader.readFully(this.value);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public boolean hasRemaining() {
			try {
				return reader.available() > 0;
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public byte[] toBytes() {
			int kl = key.length;
			int vl = value.length;
			ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + kl + vl);
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
