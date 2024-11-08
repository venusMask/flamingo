package org.apache.flamingo.memtable;

import lombok.Getter;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.lsm.FlamingoLSM;
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
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @Author venus
 * @Date 2024/11/5
 * @Version 1.0
 */
@Getter
public class DefaultMemTable implements AutoCloseable {

	private final ConcurrentSkipListMap<byte[], byte[]> memTable;

	private MemTable.MemTableState state = MemTable.MemTableState.Active;

	private final int memTableSize;

	private final WALWriter walWriter;

	private final Options options = Options.getInstance();

	private final FlamingoLSM flamingoLSM;

	public DefaultMemTable(FlamingoLSM flamingoLSM) {
		this.flamingoLSM = flamingoLSM;
		memTable = new ConcurrentSkipListMap<>((o1, o2) -> {
			int minLen = Math.min(o1.length, o2.length);
			for (int i = 0; i < minLen; i++) {
				int result = Byte.compare(o1[i], o2[i]);
				if (result != 0) {
					return result;
				}
			}
			return Integer.compare(o1.length, o2.length);
		});
		this.memTableSize = Integer.parseInt(Options.MemTableThresholdSize.getValue());
		this.walWriter = new WALWriter(this);
	}

	public void add(byte[] key, byte[] value) {
		memTable.put(key, value);
		walWriter.append(key, value);
	}

	public int size() {
		return memTable.size();
	}

	public void delete(byte[] key) {
		memTable.remove(key);
	}

	public byte[] search(byte[] key) {
		return memTable.get(key);
	}

	public void writeToSSTable() {
		state = MemTable.MemTableState.Dead;
		walWriter.changeState();
		FileChannel fileChannel = null;
		try {
			String fileName = FileUtil.getSSTFilePath();
			fileChannel = new FileOutputStream(fileName, true).getChannel();
			for (Map.Entry<byte[], byte[]> entry : memTable.entrySet()) {
				byte[] key = entry.getKey();
				byte[] value = entry.getValue();
				int kl = key.length;
				int vl = value.length;
				ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + kl + vl);
				byteBuffer.putInt(kl);
				byteBuffer.put(key);
				byteBuffer.putInt(vl);
				byteBuffer.put(value);
				byteBuffer.flip();
				try {
					fileChannel.write(byteBuffer);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			SSTable ssTable = new SSTable(fileName, 0);
			flamingoLSM.getSstMetadata().addFirstLevel(ssTable);
			walWriter.delete();
		}
		catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		finally {
			if (fileChannel != null && fileChannel.isOpen()) {
				try {
					fileChannel.close();
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public void close() throws Exception {
		walWriter.close();
	}

	public static DefaultMemTable restoreFromWAL(FlamingoLSM lsm, String walLogPath) {
		DefaultMemTable restoreMemTable = new DefaultMemTable(lsm);
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

}
