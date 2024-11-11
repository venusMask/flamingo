package org.apache.flamingo.memtable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.lsm.FlamingoLSM;
import org.apache.flamingo.memtable.skiplist.SLNode;
import org.apache.flamingo.memtable.skiplist.SkipList;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.sstable.SSTable;
import org.apache.flamingo.wal.WALWriter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Getter
public class MemoryTable implements AutoCloseable {

	private final SkipList memoryTable;

	private MemoryTableState state = MemoryTableState.Active;

	private final int memTableSize;

	private final WALWriter walWriter;

	private final Options options = Options.getInstance();

	private final FlamingoLSM flamingoLSM;

	public MemoryTable(FlamingoLSM flamingoLSM) {
		this.flamingoLSM = flamingoLSM;
		memoryTable = new SkipList();
		this.memTableSize = Integer.parseInt(Options.MemoryTableThresholdSize.getValue());
		this.walWriter = new WALWriter(this);
	}

	public void add(byte[] key, byte[] value) {
//		memoryTable.put(key, value);
		walWriter.append(key, value);
	}

	public int size() {
		return memoryTable.getSize();
	}

	public void delete(byte[] key) {
//		memoryTable.remove(key);
	}

	public byte[] search(byte[] key) {
//		return memoryTable.search(key).getValue();
		return new byte[0];
	}

	public void writeToSSTable() {
//		state = MemoryTableState.Immutable;
//		walWriter.changeState();
//		String fileName = FileUtil.getSSTFilePath();
//		try (FileChannel fileChannel = new FileOutputStream(fileName, true).getChannel()) {
//			SLNode lastHead = memoryTable.getLastHead();
//			while (lastHead.getRight().getKey() != SLNode.TailKey) {
//				lastHead = lastHead.getRight();
//				byte[] key = lastHead.getKey();
//				byte[] value = lastHead.getValue();
//				int kl = key.length;
//				int vl = value.length;
//				ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + kl + vl);
//				byteBuffer.putInt(kl);
//				byteBuffer.put(key);
//				byteBuffer.putInt(vl);
//				byteBuffer.put(value);
//				byteBuffer.flip();
//				try {
//					fileChannel.write(byteBuffer);
//				}
//				catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//			}
//			SSTable ssTable = new SSTable(fileName, 0);
//			flamingoLSM.getSstMetadata().addFirstLevel(ssTable);
//			walWriter.delete();
//		}
//		catch (IOException ignore) {
//
//		}
	}

	@Override
	public void close() throws Exception {
		walWriter.close();
	}

	public static MemoryTable restoreFromWAL(FlamingoLSM lsm, String walLogPath) {
		log.info("Restoring memory table from wal file {}", walLogPath);
		MemoryTable restoreMemTable = new MemoryTable(lsm);
		try {
			FileInputStream fileInputStream = new FileInputStream(walLogPath);
			FileChannel readChannel = fileInputStream.getChannel();
			int available = fileInputStream.available();
			ByteBuffer byteBuffer = ByteBuffer.allocate(available);
			readChannel.read(byteBuffer);
			byteBuffer.flip();
			while (true) {
				byte[] keyByte = readByteBuffer(byteBuffer);
				if (keyByte == null) {
					break;
				}
				byte[] valueByte = readByteBuffer(byteBuffer);
				restoreMemTable.add(keyByte, valueByte);
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return restoreMemTable;
	}

	public static byte[] readByteBuffer(ByteBuffer byteBuffer) {
		try {
			int fieldSize = byteBuffer.getInt();
			byte[] bytes = new byte[fieldSize];
			byteBuffer.get(bytes);
			return bytes;
		}
		catch (Exception e) {
			return null;
		}
	}

	public enum MemoryTableState {

		Active(1), Immutable(2);

		public final int state;

		MemoryTableState(int state) {
			this.state = state;
		}

	}

}
