package org.apache.flamingo.memtable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.bean.VLogAddress;
import org.apache.flamingo.bean.VLogEntity;
import org.apache.flamingo.core.Context;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.file.NamedUtil;
import org.apache.flamingo.lsm.FlamingoLSM;
import org.apache.flamingo.bean.SLNode;
import org.apache.flamingo.meta.MetaInfo;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.meta.SSTMetaInfo;
import org.apache.flamingo.utils.Pair;
import org.apache.flamingo.writer.VLogWriter;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

@Slf4j
@Getter
public class MemoryTable implements AutoCloseable {

	private final SkipList skipList;

	private MemoryTableState state = MemoryTableState.Active;

	private final int memTableSize;

	private final int maxValueSize;

	private final VLogWriter writer;

	private final Options options = Options.getInstance();

	private final MetaInfo metaInfo = Context.getInstance().getMetaInfo();

	public MemoryTable() {
		this.skipList = new SkipList();
		this.maxValueSize = Integer.parseInt(Options.MaxValueSize.getValue());
		this.memTableSize = Integer.parseInt(Options.MemoryTableThresholdSize.getValue());
		this.writer = new VLogWriter();
	}

	/**
	 * 写入流程:
	 * 1: 首先写入value_log(当作持久化日志写入)
	 * 2: 写入SkipList
	 * 3: 判断SkipList的元素数量是否超过了阈值， 如果超过则溢血到Level_0层的磁盘.
	 *
	 * @param key
	 * @param value
	 */
	public void add(byte[] key, byte[] value) throws IOException {
		VLogEntity entity = VLogEntity.from(key, value, false);
		VLogAddress address = writer.write(entity);
		SLNode node = SLNode.builder()
				.key(key)
				.value(value)
				.address(address)
				.deleted(false)
				.build();
		if(value.length > maxValueSize) {
			node.setStoreMode(true);
		}
		skipList.put(node);
	}

	public int size() {
		return skipList.getSize();
	}

	/**
	 * 删除的数据一定不是kv分离的
	 */
	public void delete(byte[] key) throws IOException {
		VLogEntity entity = VLogEntity.from(key, null, true);
		VLogAddress address = writer.write(entity);
		SLNode node = SLNode.builder()
				.key(key)
				.address(address)
				.deleted(true)
				.storeMode(false)
				.build();
		skipList.put(node);
	}

	public SLNode search(byte[] key) {
		return skipList.search(key);
	}

	/**
	 * 将内存表中的数据刷新到磁盘上
	 */
	public void flush() {
		state = MemoryTableState.Immutable;
		Pair<String, Long> pair = NamedUtil.getKeyFilePath();
		SSTMetaInfo sst = SSTMetaInfo.builder()
				.fileName(pair.getF0())
				.id(String.valueOf(pair.getF1()))
				.level(0)
				.build();
		skipList.flush(sst);
	}

	public static MemoryTable buildFromWAL(FlamingoLSM lsm, String walLogPath) {
		log.info("Restoring memory table from wal file {}", walLogPath);
		// MemoryTable restoreMemTable = new MemoryTable(lsm);
		MemoryTable restoreMemTable = new MemoryTable();
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

	@Override
	public void close() {
		flush();
		writer.close();
		// If the file is completely empty when stopped, it can be deleted directly
		String activeFullPath = writer.getActiveFullPath();
		FileUtil.deleteIfEmpty(activeFullPath);
	}

}
