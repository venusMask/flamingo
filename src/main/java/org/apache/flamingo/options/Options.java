package org.apache.flamingo.options;

public class Options {

    private static final Options INSTANCE = new Options();

    private Options(){}

    public static final Option PROJECT_NAME = Option.builder()
            .key("project_name")
            .value("flamingo")
            .doc("This project name.")
            .build();

    public static final Option PROJECT_VERSION = Option.builder()
            .key("project_version")
            .value("0.1")
            .doc("project version")
            .build();

    public static Option DATA_DIR = Option.builder()
            .key("data_dir")
            .value("data")
            .doc("data dir")
            .build();

    public static Option MEM_SIZE = Option.builder()
            .key("mem_table_size")
            .value(String.valueOf(10))
            .doc("memTable size")
            .build();

    public static void setDataDir(String dataDir) {
        DATA_DIR.setValue(dataDir);
    }

    public static Options getInstance() {
        return INSTANCE;
    }

}
