package org.apache.flamingo.sstable;

import junit.framework.TestCase;

import java.nio.charset.StandardCharsets;

/**
 * @Author venus
 * @Date 2024/11/12
 * @Version 1.0
 */
public class SSTableTest extends TestCase {

	public void testSSTableInfoSer() throws Exception {
		String fileName = "/User/compact/a.sst";
		int level = 1;
		// SSTableInfo.MetaInfo metaInfo = new SSTableInfo.MetaInfo();
		// metaInfo.setMinimumValue("min value".getBytes(StandardCharsets.UTF_8));
		// metaInfo.setMaximumValue("max value".getBytes(StandardCharsets.UTF_8));
		// metaInfo.setCreateTime(System.currentTimeMillis());
		// metaInfo.setLastUseTime(System.currentTimeMillis());
		SSTableInfo.MetaInfo metaInfo = SSTableInfo.MetaInfo.builder()
			.minimumValue("min value".getBytes(StandardCharsets.UTF_8))
			.maximumValue("max value".getBytes(StandardCharsets.UTF_8))
			.createTime(System.currentTimeMillis())
			.lastUseTime(System.currentTimeMillis())
			.build();
		SSTableInfo ssTableInfo = new SSTableInfo(fileName, level);
		ssTableInfo.setMetaInfo(metaInfo);
		byte[] serialize = SSTableInfo.serialize(ssTableInfo);
		SSTableInfo deserialize = SSTableInfo.deserialize(serialize);
		System.out.println(deserialize);
	}

}
