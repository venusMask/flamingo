package org.apache.flamingo.lsm;

import lombok.Getter;
import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.memtable.MemoryTable;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.sstable.SSTMetaInfo;
import org.apache.flamingo.sstable.SSTableInfo;
import org.apache.flamingo.task.MemoryTableTask;
import org.apache.flamingo.task.TaskManager;
import org.apache.flamingo.wal.WALWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@Getter
public class FlamingoLSM implements AutoCloseable {

	private final int memoryTableThresholdSize;

	private MemoryTable memoryTable;

	private final SSTMetaInfo sstMetadata;

	private final TaskManager taskManager;

	public FlamingoLSM() {
		this.memoryTableThresholdSize = Integer.parseInt(Options.MemoryTableThresholdSize.getValue());
		this.taskManager = new TaskManager();
		this.sstMetadata = new SSTMetaInfo();
		initMeta();
		restoreFromWAL();
		if (this.memoryTable == null) {
			this.memoryTable = new MemoryTable(this);
		}
		taskManager.start();
	}

	public void initMeta() {
		String sstRegex = SSTableInfo.SSTABLE + "(\\d+)\\.sst";
		IDAssign.initSSTAssign(FileUtil.getMaxOrder(sstRegex));
		String walRegex = WALWriter.ACTIVE + "(\\d+)\\.wal";
		IDAssign.initWALAssign(FileUtil.getMaxOrder(walRegex));
	}

	public boolean add(byte[] key, byte[] value) {
		memoryTable.add(key, value);
		if (memoryTable.size() > memoryTableThresholdSize) {
			MemoryTableTask task = new MemoryTableTask(memoryTable);
			taskManager.addTask(task);
			memoryTable = new MemoryTable(this);
		}
		return true;
	}

	public boolean delete(byte[] key) {
		memoryTable.delete(key);
		return true;
	}

	public byte[] search(byte[] key) {
		return memoryTable.search(key);
	}

	/**
	 * Recovering information from the previous legacy data
	 */
	public void restoreFromWAL() {
		restoreActiveFile();
		String rootDir = FileUtil.getDataDirPath();
		try (Stream<Path> sp = Files.list(Paths.get(rootDir))) {
			sp.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().startsWith(WALWriter.SILENCE))
				.sorted((o1, o2) -> Integer.compare(extractKey(o1), extractKey(o2)))
				.forEach(this::restoreSilenceFile);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private int extractKey(Path path) {
		String string = path.getFileName().toString();
		int lstIndex = string.lastIndexOf('_');
		String keyStr = string.substring(lstIndex + 1, string.length() - 4);
		return Integer.parseInt(keyStr);
	}

	private void restoreActiveFile() {
		String rootDir = FileUtil.getDataDirPath();
		try (Stream<Path> sp = Files.list(Paths.get(rootDir))) {
			sp.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().startsWith(WALWriter.ACTIVE))
				.findFirst()
				.ifPresent(path -> {
					String fileName = path.getFileName().toString();
					memoryTable = MemoryTable.buildFromWAL(this, FileUtil.getDataDirFilePath(fileName));
				});
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void restoreSilenceFile(Path path) {
		String silenceFileName = path.getFileName().toString();
		MemoryTable silenceMemTable = MemoryTable.buildFromWAL(this, FileUtil.getDataDirFilePath(silenceFileName));
		MemoryTableTask task = new MemoryTableTask(silenceMemTable);
		taskManager.addTask(task);
	}

	public void flush() {
		MemoryTableTask task = new MemoryTableTask(memoryTable);
		taskManager.addTask(task);
		memoryTable = new MemoryTable(this);
	}

	@Override
	public void close() throws Exception {
		flush();
		memoryTable.close();
		taskManager.close();
	}

}
