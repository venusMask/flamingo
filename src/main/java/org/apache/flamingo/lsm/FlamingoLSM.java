package org.apache.flamingo.lsm;

import lombok.Getter;
import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.memtable.DefaultMemTable;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.sstable.SSTMetadata;
import org.apache.flamingo.sstable.SSTable;
import org.apache.flamingo.task.MemTableTask;
import org.apache.flamingo.task.TaskManager;
import org.apache.flamingo.wal.WALWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@Getter
public class FlamingoLSM implements AutoCloseable {

    private final int memTableSize;

	private DefaultMemTable memTable;

	private final SSTMetadata sstMetadata;

	private final TaskManager taskManager;

	public FlamingoLSM() {
		this.memTableSize = Integer.parseInt(Options.MemTableThresholdSize.getValue());
		this.taskManager = new TaskManager();
		this.sstMetadata = new SSTMetadata();
		initMeta();
		taskManager.start();
		restoreFromWAL();
		if(this.memTable == null) {
			this.memTable = new DefaultMemTable(this);
		}
	}

	public void initMeta() {
		String sstRegex = SSTable.SSTABLE + "(\\d+)\\.sst";
		IDAssign.initSSTAssign(FileUtil.getMaxOrder(sstRegex));
		String walRegex = WALWriter.ACTIVE + "(\\d+)\\.wal";
		IDAssign.initWALAssign(FileUtil.getMaxOrder(walRegex));
	}

	/**
	 * MemTable的大小超过阈值的时候将当前memTable设置为不可变对象, 然后新构建一个MemTable接受新的请求.
	 */
	public boolean add(byte[] key, byte[] value) {
		memTable.add(key, value);
		if (memTable.size() > memTableSize) {
			MemTableTask task = new MemTableTask(memTable);
			taskManager.addTask(task);
			memTable = new DefaultMemTable(this);
		}
		return true;
	}

	public boolean delete(byte[] key) {
		memTable.delete(key);
		return true;
	}

	public byte[] search(byte[] key) {
		return memTable.search(key);
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
					memTable = DefaultMemTable.restoreFromWAL(this, path.getFileName().toString());
				});
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void restoreSilenceFile(Path path) {
		DefaultMemTable silenceMemTable = DefaultMemTable.restoreFromWAL(this, path.getFileName().toString());
		MemTableTask task = new MemTableTask(silenceMemTable);
		taskManager.addTask(task);
	}

    @Override
    public void close() throws Exception {
        memTable.close();
    }
}
