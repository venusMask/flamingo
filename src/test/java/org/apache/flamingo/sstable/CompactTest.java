package org.apache.flamingo.sstable;

import junit.framework.TestCase;
import org.apache.flamingo.utils.GeneratorDataUtil;
import org.apache.flamingo.utils.StringUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * @Author venus
 * @Date 2024/11/12
 * @Version 1.0
 */
public class CompactTest extends TestCase {

	private void prepareCompactData(String fileName, String maskFile) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(maskFile, true));
		DataOutputStream outputStream = new DataOutputStream(
				new BufferedOutputStream(Files.newOutputStream(Paths.get(fileName))));
		ArrayList<byte[]> keys = GeneratorDataUtil.generateRandomBytes(100, 5, 20);
		keys.sort(StringUtil::compareByteArrays);
		keys.forEach(pair -> {
			try {
				outputStream.writeInt(pair.length);
				outputStream.write(pair);
				outputStream.writeInt(pair.length);
				outputStream.write(pair);
				writer.write(StringUtil.fromBytes(pair) + "\r\n");
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
		writer.close();
		outputStream.close();
	}

	private SSTableInfo prepareSSTable(String fileName, String maskFile) throws IOException {
		prepareCompactData(fileName, maskFile);
		return new SSTableInfo(fileName, 0);
	}

	// private static final int MAX_ENTRIES_PER_FILE = 5;
	//
	// public void majorCompact(List<SSTableInfo> newSSTables, List<SSTableInfo>
	// oldSSTables) {
	// try {
	// List<BufferedReader> newReaders = createReaders(newSSTables);
	// List<BufferedReader> oldReaders = createReaders(oldSSTables);
	//
	// PriorityQueue<Entry> queue = new
	// PriorityQueue<>(Comparator.comparing(Entry::getKey));
	//
	// loadInitialEntries(queue, newReaders, true); // Load from newSSTables first (higher
	// priority)
	// loadInitialEntries(queue, oldReaders, false);
	//
	// BufferedWriter writer = createNewTargetWriter();
	// int entryCount = 0;
	// String lastKey = null;
	//
	// while (!queue.isEmpty()) {
	// Entry entry = queue.poll();
	//
	// // Check for duplicate keys and prioritize the entry from newSSTables
	// if (lastKey == null || !lastKey.equals(entry.key)) {
	// writer.write(entry.key + "=" + entry.value);
	// writer.newLine();
	// entryCount++;
	// lastKey = entry.key;
	//
	// if (entryCount >= MAX_ENTRIES_PER_FILE) {
	// writer.close();
	// writer = createNewTargetWriter();
	// entryCount = 0;
	// }
	// }
	//
	// // Read the next line from the SSTable and add it to the queue
	// String line = entry.reader.readLine();
	// if (line != null) {
	// queue.offer(new Entry(line, entry.reader, entry.fromNewSSTable));
	// } else {
	// entry.reader.close();
	// }
	// }
	// writer.close();
	//
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// }
	//
	// private List<BufferedReader> createReaders(List<SSTableInfo> sstables) throws
	// IOException {
	// List<BufferedReader> readers = new ArrayList<>();
	// for (SSTableInfo sstable : sstables) {
	// readers.add(new BufferedReader(new FileReader(sstable.getFileName())));
	// }
	// return readers;
	// }
	//
	// private void loadInitialEntries(PriorityQueue<Entry> queue, List<BufferedReader>
	// readers, boolean fromNewSSTable) throws IOException {
	// for (BufferedReader reader : readers) {
	// String line = reader.readLine();
	// if (line != null) {
	// queue.offer(new Entry(line, reader, fromNewSSTable));
	// }
	// }
	// }
	//
	// private BufferedWriter createNewTargetWriter() throws IOException {
	// String targetFileName =
	// "/Users/dzh/software/java/projects/flamingo/data/compact/target_" +
	// UUID.randomUUID() + ".sst";
	// return new BufferedWriter(new FileWriter(targetFileName));
	// }
	//
	// private static class Entry {
	// String key;
	// String value;
	// BufferedReader reader;
	// boolean fromNewSSTable;
	//
	// Entry(String line, BufferedReader reader, boolean fromNewSSTable) {
	// String[] parts = line.split("=", 2);
	// this.key = parts[0];
	// this.value = parts[1];
	// this.reader = reader;
	// this.fromNewSSTable = fromNewSSTable;
	// }
	//
	// public String getKey() {
	// return key;
	// }
	// }

	public void testMinHeap() {
		PriorityQueue<byte[]> priorityQueue = new PriorityQueue<>(StringUtil::compareByteArrays);
		priorityQueue.offer(StringUtil.fromString("c"));
		priorityQueue.offer(StringUtil.fromString("b"));
		priorityQueue.offer(StringUtil.fromString("a"));
		priorityQueue.offer(StringUtil.fromString("d"));
		priorityQueue.offer(StringUtil.fromString("A"));
		priorityQueue.offer(StringUtil.fromString("A"));
		while (!priorityQueue.isEmpty()) {
			byte[] poll = priorityQueue.poll();
			System.out.println(StringUtil.fromBytes(poll));
		}
	}

	public void testMajorCompact() throws IOException {
		Compact compact = new Compact();
		String maskFile = "/Users/dzh/software/java/projects/flamingo/data/compact/mask.sst";
		SSTableInfo a = prepareSSTable("/Users/dzh/software/java/projects/flamingo/data/compact/a", maskFile);
		SSTableInfo b = prepareSSTable("/Users/dzh/software/java/projects/flamingo/data/compact/b", maskFile);
		SSTableInfo c = prepareSSTable("/Users/dzh/software/java/projects/flamingo/data/compact/c", maskFile);
		SSTableInfo d = prepareSSTable("/Users/dzh/software/java/projects/flamingo/data/compact/d", maskFile);
		List<SSTableInfo> newSSTable = Arrays.asList(a, b);
		List<SSTableInfo> oldSSTable = Arrays.asList(c, d);
		compact.levelCompact(newSSTable, oldSSTable);
	}

}
