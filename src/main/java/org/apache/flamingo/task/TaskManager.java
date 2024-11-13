package org.apache.flamingo.task;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * flush mem_table to disk.
 */
@Data
@Slf4j
public class TaskManager {

	private final BlockingQueue<Task> tasks = new LinkedBlockingQueue<>();

	private volatile boolean stop = false;

	private final CountDownLatch latch;

	private Thread workerThread;

	public TaskManager() {
		latch = new CountDownLatch(1);
	}

	public void addTask(Task task) {
		try {
			log.debug("Adding task {}", task);
			tasks.put(task);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Interruption occurred while adding task!", e);
		}
	}

	public void start() {
		workerThread = new Thread(() -> {
			try {
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
			}
			finally {
				latch.countDown(); // Decrement the latch to indicate that the thread has
									// finished
			}
		});
		workerThread.start();
	}

	public void close() throws InterruptedException {
		stop = true;
		log.debug("Stopping task manager...");
		// tasks.clear(); // Optionally clear the queue to prevent further processing
		latch.await(); // Wait for the worker thread to finish
		log.debug("Task manager stopped.");
	}

}
