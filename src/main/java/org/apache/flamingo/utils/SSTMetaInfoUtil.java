package org.apache.flamingo.utils;

import org.apache.flamingo.sstable.SSTMetaInfo;

/**
 * @Author venus
 * @Date 2024/11/13
 * @Version 1.0
 */
public class SSTMetaInfoUtil {

    public static SSTMetaInfo prepareSSTMetaInfo(String filePath) {
        SSTMetaInfo metaInfo = new SSTMetaInfo(filePath);
        metaInfo.deserialize();
        return metaInfo;
    }

}
