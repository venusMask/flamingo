package org.apache.flamingo.sstable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.apache.flamingo.utils.SSTMetaInfoUtil;

import java.util.List;
import java.util.Map;

/**
 * @Author venus
 * @Date 2024/11/13
 * @Version 1.0
 */
public class SSTMetaInfoTest extends TestCase {

	private final ObjectMapper mapper = new ObjectMapper();

	public void testMetaInfo() throws JsonProcessingException {
		String filePath = "data/meta_info.meta";
		SSTMetaInfo sstMetaInfo = SSTMetaInfoUtil.parseMetaFromFile(filePath);
		Map<Integer, List<SSTableInfo>> metaInfo = sstMetaInfo.getMetaInfo();
		String prettyJSONString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(metaInfo);
		System.out.println(prettyJSONString);
	}

}
