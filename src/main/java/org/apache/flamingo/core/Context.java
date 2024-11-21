package org.apache.flamingo.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.flamingo.meta.MetaInfo;

/**
 * LSM Context
 */
@Getter
@Setter
public class Context {

	private MetaInfo metaInfo;

	private ObjectMapper objectMapper;

	private static final Context INSTANCE = new Context();

	private Context() {
	}

	public static Context getInstance() {
		return INSTANCE;
	}

}
