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
		.value(String.valueOf(3000))
		.doc("The memory table threshold size.")
		.build();

	public static Option SSTableMaxSize = Option.builder()
		.key("sstable_max_size")
		.value(String.valueOf(3000))
		.doc("The maximum number of kv pairs contained in each sstable.")
		.build();

	public static Option MaxLevel = Option.builder()
			.key("max_level")
			.value(String.valueOf(10))
			.doc("Max Level")
			.build();

	public static Option MaxValueSize = Option.builder()
			.key("max_value_size")
			.value(String.valueOf(30))
			.doc("Max Value Size")
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
