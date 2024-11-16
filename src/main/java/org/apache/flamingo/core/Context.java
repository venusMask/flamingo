package org.apache.flamingo.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.flamingo.sstable.SSTMetaInfo;

/**
 * LSM Context
 */
@Getter
@Setter
public class Context {

	private SSTMetaInfo sstMetadata;

	private ObjectMapper objectMapper;

	private static final Context INSTANCE = new Context();

	public static Context getInstance() {
		return INSTANCE;
	}

}
