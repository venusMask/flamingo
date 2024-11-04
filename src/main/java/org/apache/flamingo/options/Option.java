package org.apache.flamingo.options;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class Option {

    private String key;

    private String value;

    private String doc;

}
