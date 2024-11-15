package org.apache.flamingo.memtable;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flamingo.lsm.FlamingoLSM;
import org.apache.flamingo.memtable.skiplist.SLNode;
import org.apache.flamingo.memtable.skiplist.SkipList;
import org.apache.flamingo.utils.GeneratorDataUtil;
import org.apache.flamingo.utils.StringUtil;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.flamingo.memtable.MemoryTable.readByteBuffer;

/**
 * Test Skip List
 *
 * @Author venus
 * @Date 2024/11/5
 * @Version 1.0
 */
@Slf4j
public class SkipListTest extends TestCase {

	private boolean assertKey(List<String> accept, List<String> real) {
		return accept.containsAll(real) && real.containsAll(accept);
	}

	private void put(SkipList skipList, String key, String value) {
		skipList.put(key, value);
	}

	public void testDuplicateKey() {
		SkipList skipList = new SkipList(0.5, 3);
		for (int i = 0; i < 10; i++) {
			skipList.put(String.valueOf(i), String.valueOf(i));
		}
		System.out.println(skipList);
		for (int i = 0; i < 10; i++) {
			skipList.put(String.valueOf(i), String.valueOf(i + 100));
		}
		System.out.println(skipList);
	}

	public void testSimpleOperation() throws Exception {
		SkipList skipList = new SkipList();
		// Test PUT
		ArrayList<String> col = GeneratorDataUtil.generateRandomStrings(10000, 1, 10);
		long start = System.currentTimeMillis();
		for (String s : col) {
			put(skipList, s, s);
		}
		long end = System.currentTimeMillis();
		log.info("Put took {} ms", end - start);
		boolean putFlag = assertKey(col, skipList.keys());
		assertTrue(putFlag);
		// Test Remove
		Random random = new Random();
		HashSet<String> hashSet = new HashSet<>();
		long start1 = System.currentTimeMillis();
		for (int i = 0; i < 30; i++) {
			String key = col.get(random.nextInt(col.size()));
			skipList.remove(key);
			hashSet.add(key);
		}
		long end1 = System.currentTimeMillis();
		log.info("Remove took {} ms", end1 - start1);
		col.removeAll(hashSet);
		boolean removeFlag = assertKey(col, skipList.keys());
		assertTrue(removeFlag);
		// Test Search
		List<String> keys = skipList.keys();
		log.debug("Test contains!");
		long start2 = System.currentTimeMillis();
		keys.forEach(key -> {
			SLNode value = skipList.search(key);
			assertEquals(key, StringUtil.fromBytes(value.getKey()));
		});
		long end2 = System.currentTimeMillis();
		log.info("Search(contains) took {} ms", end2 - start2);
		log.debug("Test not contains!");
		ArrayList<String> randomKeys = GeneratorDataUtil.generateRandomStrings(10, 1, 10);
		long start3 = System.currentTimeMillis();
		randomKeys.forEach(key -> {
			skipList.search(key);
			assertTrue(true);
		});
		long end3 = System.currentTimeMillis();
		log.info("Search(not contains) took {} ms", end3 - start3);
	}

}
