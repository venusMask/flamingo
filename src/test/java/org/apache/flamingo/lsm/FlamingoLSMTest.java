package org.apache.flamingo.lsm;

import junit.framework.TestCase;
import org.apache.flamingo.utils.GeneratorDataUtil;

import java.util.ArrayList;

public class FlamingoLSMTest extends TestCase {

	public void testSimpleOperation() throws Exception {
		int len = 100;
		try (FlamingoLSM flamingoLSM = new FlamingoLSM()) {
			ArrayList<byte[]> keys = GeneratorDataUtil.generateRandomBytes(len, 20, 100);
			long start = System.currentTimeMillis();
			for (int i = 0; i < len; i++) {
				flamingoLSM.add(keys.get(i), keys.get(i));
			}
			long end = System.currentTimeMillis();
			System.out.println(end - start);
		}
	}

}
