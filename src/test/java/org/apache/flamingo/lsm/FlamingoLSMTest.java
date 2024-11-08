package org.apache.flamingo.lsm;

import junit.framework.TestCase;
import org.apache.flamingo.memtable.SkipListTest;

import java.util.ArrayList;

public class FlamingoLSMTest extends TestCase {

	public void testFlamingoLSM() throws Exception {
		try (FlamingoLSM flamingoLSM = new FlamingoLSM()) {
			int len = 12000;
			ArrayList<byte[]> keyList = SkipListTest.testData(len);
			ArrayList<byte[]> valueList = SkipListTest.testData(len);
			long start = System.currentTimeMillis();
			for (int i = 0; i < len; i++) {
				byte[] key = keyList.get(i);
				byte[] value = valueList.get(i);
				flamingoLSM.add(key, value);
			}
			long end = System.currentTimeMillis();
			System.out.println(end - start);
		}
	}

}
