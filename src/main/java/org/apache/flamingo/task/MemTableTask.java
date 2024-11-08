package org.apache.flamingo.task;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.memtable.DefaultMemTable;

@Slf4j
@Builder
public class MemTableTask implements Task {

	private final DefaultMemTable memTable;

	public MemTableTask(DefaultMemTable memTable) {
		this.memTable = memTable;
	}

	@Override
	public void start() {

	}

	@Override
	public void execute() {
		memTable.writeToSSTable();
	}

	@Override
	public void stop() {

	}

}
