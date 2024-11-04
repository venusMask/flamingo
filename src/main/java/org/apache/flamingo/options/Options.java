package org.apache.flamingo.options;

public class Options {

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

}
