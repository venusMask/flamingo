package org.apache.flamingo.task;

/**
 * @Author venus
 * @Date 2024/11/8
 * @Version 1.0
 */
public interface Task {

	void start();

	void execute();

	void stop();

	default void executeTask() {
		start();
		execute();
		stop();
	}

}
