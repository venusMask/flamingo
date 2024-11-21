package org.apache.flamingo.lsm;

import junit.framework.TestCase;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.utils.GeneratorDataUtil;
import org.apache.flamingo.utils.StringUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

public class FlamingoLSMTest extends TestCase {

	private void assertKey(byte[] cap, String key) {
		assertTrue(Arrays.equals(key.getBytes(StandardCharsets.UTF_8), cap));
	}

	private void assertNotKey(byte[] cap, String key) {
		assert cap == null;
	}

    public void testLSMAdd() {
		int pariSize = 10000;
        try (FlamingoLSM lsm = new FlamingoLSM()) {
            for (int i = 0; i < pariSize; i++) {
				lsm.add(
						StringUtil.fromString(String.valueOf(i)),
						StringUtil.fromString(String.valueOf(i))
				);
			}
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//	public void testPipeline() throws Exception {
//		String dirValue = Options.DataDir.getValue();
//		Path path = Paths.get(dirValue);
//		FileUtil.deleteDirectory(path);
//		Files.createDirectory(path);
//		try (FlamingoLSM lsm = new FlamingoLSM()) {
//			for (int i = 0; i < 30; i++) {
//				lsm.add(
//						StringUtil.fromString(String.valueOf(i)),
//						StringUtil.fromString(String.valueOf(i))
//				);
//			}
//			byte[] search_1 = lsm.search(StringUtil.fromString(String.valueOf(1)));
//			assertKey(search_1, "1");
//
//			byte[] search_10 = lsm.search(StringUtil.fromString(String.valueOf(1)));
//			assertKey(search_10, "10");
//
//			byte[] search_29 = lsm.search(StringUtil.fromString(String.valueOf(1)));
//			assertKey(search_29, "29");
//
//			byte[] search_30 = lsm.search(StringUtil.fromString(String.valueOf(1)));
//			assertNotKey(search_29, "30");
//		}
//	}

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
