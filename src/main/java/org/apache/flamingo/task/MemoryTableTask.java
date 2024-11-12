package org.apache.flamingo.task;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.memtable.MemoryTable;

@Slf4j
@Builder
public class MemoryTableTask implements Task {

	private final MemoryTable memoryTable;

	public MemoryTableTask(MemoryTable memTable) {
		this.memoryTable = memTable;
	}

	@Override
	public void start() {

	}

	@Override
	public void execute() {
		memoryTable.flush();
	}

	@Override
	public void stop() {

	}

	@Override
	public String toString() {
		return "MemoryTableTask{" + "memoryTable=" + memoryTable.getWalWriter().getWalActiveFullPath() + '}';
	}

}
