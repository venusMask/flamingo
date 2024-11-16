package org.apache.flamingo.memtable.skiplist;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.annotation.ForTest;
import org.apache.flamingo.memtable.MemoryTable;
import org.apache.flamingo.sstable.SSTableInfo;
import org.apache.flamingo.utils.StringUtil;

import java.io.FileInputStream;
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

	public void put(String key, String value) {
		put(StringUtil.fromString(key), StringUtil.fromString(value));
	}

	public void put(byte[] key, byte[] value) {
		put(key, value, false);
	}

	public void remove(String key) {
		remove(StringUtil.fromString(key));
	}

	public void remove(byte[] key) {
		put(key, null, true);
	}

	private void put(byte[] key, byte[] value, boolean isDeleted) {
		SLNode anchorNode = findPrev(key);
		// The target node exists
		if (Arrays.equals(key, anchorNode.getKey())) {
			anchorNode.setDeleted(isDeleted);
			anchorNode.setValue(value);
			return;
		}
		SLNode insertNode = new SLNode(key, value, isDeleted);
		afterInsert(anchorNode, insertNode);
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
			SLNode e = new SLNode(key, null);
			afterInsert(anchorNode, e);
			vertical(e, insertNode);
			insertNode = e;
			currentLevel++;
		}
		size++;
	}

	public SLNode search(String key) {
		return search(StringUtil.fromString(key));
	}

	public SLNode search(byte[] key) {
		SLNode p = findPrev(key);
		if (Arrays.equals(key, p.getKey())) {
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

	public static SkipList build(SkipListOption option, SSTableInfo ssTable) {
		SkipList skipList = new SkipList(option);
		try (FileInputStream fileInputStream = new FileInputStream(ssTable.getFileName())) {
			FileChannel readChannel = fileInputStream.getChannel();
			int available = fileInputStream.available();
			ByteBuffer byteBuffer = ByteBuffer.allocate(available);
			readChannel.read(byteBuffer);
			byteBuffer.flip();
			while (true) {
				byte delByte = byteBuffer.get();
				byte[] keyByte = MemoryTable.readByteBuffer(byteBuffer);
				if (keyByte == null) {
					break;
				}
				byte[] valueByte = MemoryTable.readByteBuffer(byteBuffer);
				skipList.put(keyByte, valueByte, delByte == (byte) 1);
			}
			return skipList;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Flush memory data to disk. [delete_flag k_len k_val v_len v_val]
	 * @param sst target file
	 */
	public void flush(SSTableInfo sst) {
		String fileName = sst.getFileName();
		SLNode lastHead = getLastHead();
		byte[] minKey = lastHead.getRight().getKey();
		lastHead = lastHead.getRight();
		long count = 0;
		try (FileOutputStream outputStream = new FileOutputStream(fileName, true)) {
			FileChannel channel = outputStream.getChannel();
			while (lastHead.getRight() != null) {
				count++;
				byte isDeleted = lastHead.isDeleted() ? (byte) 1 : (byte) 0;
				byte[] key = lastHead.getKey();
				byte[] value = lastHead.getValue();
				int kl = key.length;
				int vl = value.length;
				int totalLength = 1 + 4 + 4 + kl + vl;
				ByteBuffer byteBuffer = ByteBuffer.allocate(totalLength);
				byteBuffer.put(isDeleted);
				byteBuffer.putInt(kl);
				byteBuffer.put(key);
				byteBuffer.putInt(vl);
				byteBuffer.put(value);
				byteBuffer.flip();
				int written = channel.write(byteBuffer);
				if (written != totalLength) {
					throw new RuntimeException("Write less");
				}
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
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
