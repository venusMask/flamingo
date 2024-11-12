package org.apache.flamingo.memtable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.lsm.FlamingoLSM;
import org.apache.flamingo.memtable.skiplist.SkipList;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.sstable.SSTableInfo;
import org.apache.flamingo.wal.WALWriter;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
		this.memoryTable = new SkipList();
		this.memTableSize = Integer.parseInt(Options.MemoryTableThresholdSize.getValue());
		this.walWriter = new WALWriter(this);
	}

	public void add(byte[] key, byte[] value) {
		memoryTable.put(key, value);
		walWriter.append(key, value);
	}

	public int size() {
		return memoryTable.getSize();
	}

	public void delete(byte[] key) {
		memoryTable.remove(key);
	}

	public byte[] search(byte[] key) {
		return memoryTable.search(key);
	}

	public void flush() {
		state = MemoryTableState.Immutable;
		walWriter.changeState();
		String fileName = FileUtil.getSSTFileName();
		SSTableInfo ssTable = new SSTableInfo(fileName, 0);
		memoryTable.flush(ssTable);
		flamingoLSM.getSstMetadata().addFirstLevel(ssTable);
		walWriter.delete();
	}

	@Override
	public void close() throws Exception {
		walWriter.close();
	}

	public static MemoryTable buildFromWAL(FlamingoLSM lsm, String walLogPath) {
		log.info("Restoring memory table from wal file {}", walLogPath);
		MemoryTable restoreMemTable = new MemoryTable(lsm);
		try (FileInputStream fileInputStream = new FileInputStream(walLogPath)) {
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
			return restoreMemTable;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
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
