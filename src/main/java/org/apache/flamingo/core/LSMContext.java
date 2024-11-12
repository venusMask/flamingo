package org.apache.flamingo.core;

import lombok.Getter;
import lombok.Setter;
import org.apache.flamingo.sstable.SSTMetaInfo;

/**
 * LSM Context
 */
public class LSMContext {

	@Getter
	@Setter
	private SSTMetaInfo sstMetadata;

	private static final LSMContext INSTANCE = new LSMContext();

	public static LSMContext getInstance() {
		return INSTANCE;
	}

}
