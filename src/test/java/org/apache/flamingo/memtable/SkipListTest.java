package org.apache.flamingo.memtable;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.bean.SLNode;
import org.apache.flamingo.bean.VLogAddress;
import org.apache.flamingo.utils.GeneratorDataUtil;
import org.apache.flamingo.utils.StringUtil;

import java.util.*;
import java.util.function.Consumer;

/**
 * Test Skip List
 *
 * @Author venus
 * @Date 2024/11/5
 * @Version 1.0
 */
@Slf4j
public class SkipListTest extends TestCase {

	public static VLogAddress generateVLogAddress() {
		long fieldID = GeneratorDataUtil.generateLong();
		long offset = GeneratorDataUtil.generateLong();
		return VLogAddress.from(fieldID, offset);
	}

	public static SLNode generationRandomNode() {
		String key = GeneratorDataUtil.generateRandomString(1, 10);
		String value = GeneratorDataUtil.generateRandomString(1, 10);
		boolean storeMode = GeneratorDataUtil.generateBoolean();
		boolean deleted = GeneratorDataUtil.generateBoolean();
		VLogAddress vLogAddress = generateVLogAddress();
		return new SLNode(StringUtil.fromString(key), StringUtil.fromString(value), storeMode, vLogAddress, deleted);
	}

	public List<SLNode> generateSLNodes(int total) {
		ArrayList<SLNode> nodes = new ArrayList<>(total);
		for (int i = 0; i < total; i++) {
			nodes.add(generationRandomNode());
		}
		return nodes;
	}

	private void cost(Consumer<String> function, String message) {
		long startTime = System.currentTimeMillis();
		function.accept(message);
		long endTime = System.currentTimeMillis();
		log.info("Execute task: [{}], Cost time: {} ms!", message, endTime - startTime);
	}

	public void testPutSLNode() {
		int putNodeSize = 100;
		SkipList skipList = new SkipList(0.5, 3);
		List<SLNode> nodes = generateSLNodes(putNodeSize);

		cost(s -> nodes.forEach(node -> {
			node.setDeleted(false);
			skipList.put(node);
		}), "Put Node(" + putNodeSize + ")");
		log.info("Skip List graph:\n {}", skipList.graph());

		int delSize = putNodeSize / 3;
		cost(s -> {
			for (int i = 0; i < delSize; i++) {
				SLNode node = nodes.get(i);
				node.setDeleted(true);
				skipList.remove(node);
			}
		}, "Delete Node( " + delSize + ")");
		String graph = skipList.graph();
		log.info("Skip List graph:\n {}", skipList.graph());

		cost(s -> {
			for (int i = 0; i < delSize; i++) {
				SLNode node = nodes.get(i);
				SLNode search = skipList.search(node.getKey());
				assert search == null;
			}
			for (int i = delSize; i < putNodeSize; i++) {
				SLNode node = nodes.get(i);
				SLNode search = skipList.search(node.getKey());
				if (search == null) {
					log.info("i = {}", i);
				}
				assert search != null;
			}
		}, "Search Node");
	}

	public void testDuplicateKey() {
		SkipList skipList = new SkipList(0.5, 3);
		List<SLNode> nodes = generateSLNodes(10);
		cost(s -> nodes.forEach(node -> {
			node.setDeleted(false);
			skipList.put(node);
		}), "Put Node(" + 10 + ")");
		cost(s -> nodes.forEach(node -> {
			node.setDeleted(false);
			node.setValue(StringUtil.fromString("New Value"));
			skipList.put(node);
		}), "Put Node(" + 10 + ")");
		log.info("Skip List graph:\n {}", skipList.graph());
	}

}
