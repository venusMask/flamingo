package org.apache.flamingo.memtable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.annotation.ForTest;
import org.apache.flamingo.bean.SLNode;
import org.apache.flamingo.core.Context;
import org.apache.flamingo.options.SkipListOption;
import org.apache.flamingo.meta.SSTMetaInfo;
import org.apache.flamingo.utils.StringUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static org.apache.flamingo.utils.StringUtil.compareByteArrays;

@Slf4j
@Getter
public class SkipList {

	private SLNode head;

	private SLNode tail;

	private int size;

	private int level;

	private final Random random = new Random();

	private final double probability;

	private final int maxLevel;

	public SkipList(SkipListOption option) {
		this.probability = option.getProbability();
		this.maxLevel = option.getMaxLevel();
		this.head = new SLNode(SLNode.HeadKey, null);
		this.tail = new SLNode(SLNode.TailKey, null);
		this.size = 0;
		this.level = 0;
		horizontal(head, tail);
	}

	public SkipList(double probability, int maxLevel) {
		this.probability = probability;
		this.maxLevel = maxLevel;
		this.head = new SLNode(SLNode.HeadKey, null);
		this.tail = new SLNode(SLNode.TailKey, null);
		this.size = 0;
		this.level = 0;
		horizontal(head, tail);
	}

	public SkipList() {
		this(0.5, 32);
	}

//	public void put(String key, String value) {
//		put(StringUtil.fromString(key), StringUtil.fromString(value));
//	}
//
//	public void put(byte[] key, byte[] value) {
//		put(key, value, false);
//	}
//
//	public void remove(String key) {
//		remove(StringUtil.fromString(key));
//	}

//	public void remove(byte[] key) {
//		put(key, null, true);
//	}

	public void remove(SLNode deleteNode) {
		put(deleteNode);
	}

	public void put(SLNode needAddNode) {
		SLNode anchorNode = findPrev(needAddNode.getKey());
		// The target node exists
		if (Arrays.equals(needAddNode.getKey(), anchorNode.getKey())) {
			anchorNode.setDeleted(needAddNode.isDeleted());
			anchorNode.setValue(needAddNode.getValue());
			anchorNode.setStoreMode(needAddNode.isStoreMode());
			anchorNode.setAddress(needAddNode.getAddress());
			return;
		}
		afterInsert(anchorNode, needAddNode);
		int currentLevel = 0;
		while (random.nextDouble() < probability) {
			// If it exceeds the height, a new top floor needs to be built
			if (currentLevel >= level && currentLevel < maxLevel) {
				level++;
				SLNode newHead = new SLNode(SLNode.HeadKey, null);
				SLNode newTail = new SLNode(SLNode.TailKey, null);
				horizontal(newHead, newTail);
				vertical(newHead, head);
				vertical(newTail, tail);
				head = newHead;
				tail = newTail;
			}
			while (anchorNode != null && anchorNode.getUp() == null) {
				anchorNode = anchorNode.getLeft();
			}
			if (anchorNode == null) {
				break;
			}
			anchorNode = anchorNode.getUp();
			// Index nodes do not require value values
			SLNode e = new SLNode(needAddNode.getKey(), null);
			afterInsert(anchorNode, e);
			vertical(e, needAddNode);
			needAddNode = e;
			currentLevel++;
		}
		size++;
	}

	public SLNode search(String key) {
		return search(StringUtil.fromString(key));
	}

	public SLNode search(byte[] key) {
		SLNode p = findPrev(key);
		if (Arrays.equals(key, p.getKey()) && !p.isDeleted()) {
			return p;
		}
		else {
			return null;
		}
	}

	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * Find the node in front of the position to be inserted at the bottom layer.
	 * <p>
	 * newHead will encounter the following situations:
	 * <p>
	 * 1: On the head node (no node or less than the first node)
	 * <p>
	 * 2: On the target node (key exists in the linked list)
	 * <p>
	 * 3: The previous node of the target node (key does not exist in the linked list)
	 */
	private SLNode findPrev(byte[] key) {
		SLNode newHead = head;
		while (true) {
			while (!Arrays.equals(newHead.getRight().getKey(), SLNode.TailKey)
					&& compareByteArrays(newHead.getRight().getKey(), key) <= 0) {
				newHead = newHead.getRight();
			}
			if (newHead.getDown() != null) {
				newHead = newHead.getDown();
			}
			else {
				break;
			}
		}
		return newHead;
	}

	private void afterInsert(SLNode anchor, SLNode insert) {
		insert.setLeft(anchor);
		insert.setRight(anchor.getRight());
		anchor.getRight().setLeft(insert);
		anchor.setRight(insert);
	}

	private void horizontal(SLNode prev, SLNode next) {
		prev.setRight(next);
		next.setLeft(prev);
	}

	private void vertical(SLNode up, SLNode down) {
		up.setDown(down);
		down.setUp(up);
	}

	private SLNode getLastHead() {
		SLNode p = head;
		while (p.getDown() != null) {
			p = p.getDown();
		}
		return p;
	}

	public List<String> keys() {
		SLNode lastHead = getLastHead();
		lastHead = lastHead.getRight();
		List<String> keys = new LinkedList<>();
		while (lastHead.getRight() != null) {
			if (!lastHead.isDeleted()) {
				keys.add(StringUtil.fromBytes(lastHead.getKey()));
			}
			lastHead = lastHead.getRight();
		}
		return keys;
	}

	@ForTest
	public String graph() {
		StringBuilder builder = new StringBuilder();
		ArrayList<List<String>> vertical = new ArrayList<>();
		SLNode lastHead = getLastHead();
		int maxWidth = 0;
		while (lastHead != null) {
			SLNode currentNode = lastHead;
			ArrayList<String> itemArray = new ArrayList<>();
			String lastWord = null;
			for (int i = 0; i < level; i++) {
				if (currentNode == null) {
					// Virtual Node
					itemArray.add("<" + lastWord + ">");
				}
				else {
					if (lastWord == null) {
						lastWord = StringUtil.fromBytes(currentNode.getKey())
								+ (currentNode.isDeleted() ? "(T)" : "(F)");
					}
					maxWidth = Math.max(maxWidth, lastWord.length());
					itemArray.add(lastWord);
					currentNode = currentNode.getUp();
				}
			}
			lastHead = lastHead.getRight();
			vertical.add(itemArray);
		}
		final int finalWidth = maxWidth + 5;
		for (int i = level - 1; i >= 0; i--) {
			for (List<String> line : vertical) {
				builder.append(getPaddingString(finalWidth, line.get(i)));
			}
			builder.append("\n");
		}
		return builder.toString();
	}

	private String getPaddingString(int totalWith, String str) {
		int padding = (totalWith - str.length()) / 2;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < padding; i++) {
			sb.append(' ');
		}
		sb.append(str);
		for (int i = 0; i < padding; i++) {
			sb.append(' ');
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		if (size == 0) {
			return "Empty SkipList";
		}
		StringBuilder builder = new StringBuilder();
		SLNode p = head;
		while (p.getDown() != null) {
			p = p.getDown();
		}
		while (p.getLeft() != null) {
			p = p.getLeft();
		}
		// Skip Head node
		if (p.getRight() != null) {
			p = p.getRight();
		}
		while (p.getRight() != null) {
			builder.append(p);
			builder.append("\n");
			p = p.getRight();
		}
		return builder.toString();
	}

	/**
	 * Flush memory data to disk and .
	 *
	 * @param sst target file
	 */
	public void flush(SSTMetaInfo sst) {
		String fileName = sst.getFileName();
		SLNode lastHead = getLastHead();
		byte[] minKey = lastHead.getRight().getKey();
		lastHead = lastHead.getRight();
		long count = 0;
		try (FileOutputStream outputStream = new FileOutputStream(fileName, true)) {
			FileChannel channel = outputStream.getChannel();
			while (lastHead.getRight() != null) {
				count++;
				byte[] serialize = SLNode.serialize(lastHead);
				channel.write(ByteBuffer.wrap(serialize));
				lastHead = lastHead.getRight();
			}
			byte[] maxKey = lastHead.getLeft().getKey();
			sst.setMinimumValue(minKey);
			sst.setMaximumValue(maxKey);
			sst.setCount(count);
			channel.force(true);
			outputStream.flush();
			log.debug("Flush skip list to sst, file: {}, count: {}. Skip list minimum key: {}, maximum key: {}.",
					fileName, count, StringUtil.fromBytes(minKey), StringUtil.fromBytes(maxKey));
			Context.getInstance().getMetaInfo().addTable(sst);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
