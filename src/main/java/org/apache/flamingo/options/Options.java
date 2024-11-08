package org.apache.flamingo.options;

public class Options {

	private static final Options INSTANCE = new Options();

	private Options() {
	}

	public static final Option ProjectName = Option.builder()
		.key("project_name")
		.value("Flamingo LSM")
		.doc("This project name.")
		.build();

	public static final Option ProjectVersion = Option.builder()
		.key("project_version")
		.value("0.1")
		.doc("project version")
		.build();

	public static Option DataDir = Option.builder().key("data_dir").value("data").doc("data dir").build();

	public static Option MemoryTableThresholdSize = Option.builder()
		.key("memory_table_threshold_size")
		.value(String.valueOf(10000))
		.doc("memTable size")
		.build();

	public static void setDataDir(String dataDir) {
		DataDir.setValue(dataDir);
	}

	public static void setMemoryTableThresholdSize(int memoryTableThresholdSize) {
		MemoryTableThresholdSize.setValue(String.valueOf(memoryTableThresholdSize));
	}

	public static Options getInstance() {
		return INSTANCE;
	}

}
