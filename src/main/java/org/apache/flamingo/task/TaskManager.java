package org.apache.flamingo.task;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * flush mem_table to disk.
 */
@Data
@Slf4j
public class TaskManager {

	private final BlockingQueue<Task> tasks = new LinkedBlockingQueue<>();

	private volatile boolean stop = false;

	public TaskManager() {
	}

	public void addTask(Task task) {
		try {
			log.debug("Adding task {}", task);
			tasks.put(task);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Interruption occurred while adding task! ", e);
		}
	}

	public void start() {
		new Thread(() -> {
			while (!stop || !tasks.isEmpty()) {
				log.debug("Get tasks from queue and executing tasks...");
				try {
					Task task = tasks.take();
					task.executeTask();
				}
				catch (InterruptedException e) {
					log.debug("Interruption occurred while executing tasks.", e);
					Thread.currentThread().interrupt();
					if (stop) {
						break;
					}
				}
				log.debug("Execute end of tasks");
			}
		}).start();
	}

}
