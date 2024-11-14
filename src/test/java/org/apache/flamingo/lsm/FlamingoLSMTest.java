package org.apache.flamingo.lsm;

import junit.framework.TestCase;
import org.apache.flamingo.utils.GeneratorDataUtil;

import java.util.ArrayList;

public class FlamingoLSMTest extends TestCase {

	public void testSimpleOperation() throws Exception {
		int len = 102;
		long genStart = System.currentTimeMillis();
		ArrayList<byte[]> keys = GeneratorDataUtil.generateRandomBytes(len, 20, 100);
		long genEnd = System.currentTimeMillis();
		System.out.println("Generate keys cost " + (genEnd - genStart) + " ms");
		long start = System.currentTimeMillis();
		try (FlamingoLSM flamingoLSM = new FlamingoLSM()) {
			for (int i = 0; i < len; i++) {
				flamingoLSM.add(keys.get(i), keys.get(i));
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("Execute cost time: " + (end - start));
	}

}
