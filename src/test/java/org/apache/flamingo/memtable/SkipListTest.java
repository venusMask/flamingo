package org.apache.flamingo.memtable;

import junit.framework.TestCase;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flamingo.lsm.FlamingoLSM;
import org.apache.flamingo.memtable.skiplist.SLNode;
import org.apache.flamingo.memtable.skiplist.SkipList;
import org.apache.flamingo.utils.StringUtil;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.flamingo.memtable.MemoryTable.readByteBuffer;

/**
 * Test Skip List
 * @Author venus
 * @Date 2024/11/5
 * @Version 1.0
 */
public class SkipListTest extends TestCase {

	private boolean assertKey(List<String> accept, List<String> real) {
		return accept.containsAll(real) && real.containsAll(accept);
	}

	public static ArrayList<String> generateRandomStrings(int count) {
		RandomStringUtils randomStringUtils = RandomStringUtils.secure();
		ArrayList<String> list = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			list.add(randomStringUtils.nextAlphabetic(1, 5));
		}
		return list;
	}

	public void put(SkipList skipList, String key, String value) {
		skipList.put(key, value);
	}

	public void testSimpleOperation() throws Exception {
		SkipList skipList = new SkipList();
		// Test PUT
		ArrayList<String> col = generateRandomStrings(20);
		for (String s : col) {
			put(skipList, s, s);
		}
		boolean putFlag = assertKey(col, skipList.keys());
        assertTrue(putFlag);
		// Test Remove
		Random random = new Random();
		HashSet<String> hashSet = new HashSet<>();
		for (int i = 0; i < 10; i++) {
			String key = col.get(random.nextInt(col.size()));
			skipList.remove(key);
			hashSet.add(key);
		}
		col.removeAll(hashSet);
		boolean removeFlag = assertKey(col, skipList.keys());
		assertTrue(removeFlag);
		// Test Search
		List<String> keys = skipList.keys();
		System.out.println("Test contains!");
		keys.forEach(key -> {
			byte[] value = skipList.search(key);
			assertEquals(key, StringUtil.fromBytes(value));
		});
		System.out.println("Test not contains!");
		ArrayList<String> randomKeys = generateRandomStrings(10);
		randomKeys.forEach(key -> {
			skipList.search(key);
			assertTrue(true);
		});
	}

}
