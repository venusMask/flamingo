package org.apache.flamingo.lsm;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.bean.SLNode;
import org.apache.flamingo.core.Context;
import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.file.NamedUtil;
import org.apache.flamingo.memtable.MemoryTable;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.meta.MetaInfo;
import org.apache.flamingo.task.MemoryTableTask;
import org.apache.flamingo.task.TaskManager;
import org.apache.flamingo.wal.WALWriter;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@Slf4j
@Getter
public class FlamingoLSM implements AutoCloseable {

	private final int memoryTableThresholdSize;

	private MemoryTable memoryTable;

	private MetaInfo metaInfo;

	private final TaskManager taskManager;

	public FlamingoLSM() {
		ObjectMapper objectMapper = new ObjectMapper();
		Context.getInstance().setObjectMapper(objectMapper);
		this.memoryTableThresholdSize = Integer.parseInt(Options.MemoryTableThresholdSize.getValue());
		this.taskManager = new TaskManager();
		this.memoryTable = new MemoryTable();
		this.metaInfo = new MetaInfo();
		Context.getInstance().setMetaInfo(metaInfo);
		init();
		taskManager.start();
	}

	private void init() {
		try {
			Files.createDirectories(Paths.get(NamedUtil.getKeyDir()));
			Files.createDirectories(Paths.get(NamedUtil.getValueDir()));
			FileUtil.checkFileExistsOrCreate(NamedUtil.getMetaDir());
			int keyMaxOrder = FileUtil.getMaxOrder(NamedUtil.getKeyDir(), "(\\d+)\\.sst");
			int valMaxOrder = FileUtil.getMaxOrder(NamedUtil.getValueDir(), "(\\d+)\\.wal");
			IDAssign.initSSTAssign(keyMaxOrder);
			IDAssign.initWALAssign(valMaxOrder);
			this.metaInfo = MetaInfo.deserialize(NamedUtil.getMetaDir());
		} catch (FileAlreadyExistsException ignore) {
			// Ignore exception
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean add(byte[] key, byte[] value) throws IOException {
		memoryTable.add(key, value);
		if (memoryTable.size() > memoryTableThresholdSize) {
			MemoryTableTask task = new MemoryTableTask(memoryTable);
			taskManager.addTask(task);
			log.debug("Switch memory table");
			memoryTable = new MemoryTable();
		}
		return true;
	}

	public boolean delete(byte[] key) {
        try {
            memoryTable.delete(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
	}

	public byte[] search(byte[] key) {
		SLNode memory = memoryTable.search(key);
		if (memory != null && memory.isDeleted()) {
			return null;
		}
		// Search from disk
		return metaInfo.search(key);
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

	public void flush(boolean terminal) {
		MemoryTableTask task = new MemoryTableTask(memoryTable);
		taskManager.addTask(task);
		if (!terminal) {
			memoryTable = new MemoryTable();
		}
	}

	@Override
	public void close() {
		flush(true);
		memoryTable.close();
        try {
            taskManager.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.debug("Closing FlamingoLSM Success!");
	}

}
