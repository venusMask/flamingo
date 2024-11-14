package org.apache.flamingo.utils;

import org.apache.flamingo.sstable.SSTMetaInfo;

public class SSTMetaInfoUtil {

	public static SSTMetaInfo parseMetaFromFile(String filePath) {
		return new SSTMetaInfo(filePath);
	}

}
