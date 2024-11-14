package org.apache.flamingo.memtable.skiplist;

import lombok.Getter;
import lombok.Setter;

import java.nio.charset.StandardCharsets;

/**
 * Skip List Node
 */
@Getter
@Setter
public class SLNode {

	public static final byte[] HeadKey = "HEAD".getBytes(StandardCharsets.UTF_8);

	public static final byte[] TailKey = "TAIL".getBytes(StandardCharsets.UTF_8);

	private byte[] key;

	private byte[] value;

	private boolean deleted = false;

	private SLNode right;

	private SLNode left;

	private SLNode up;

	private SLNode down;

	public SLNode(byte[] key, byte[] value) {
		this(key, value, false);
	}

	public SLNode(byte[] key, byte[] value, boolean deleted) {
		this.key = key;
		this.value = value;
		this.deleted = deleted;
	}

	public SLNode() {
	}

	@Override
	public String toString() {
		String keyString = new String(key, StandardCharsets.UTF_8);
		String valueString;
		if (value == null) {
			valueString = "";
		}
		else {
			valueString = new String(value, StandardCharsets.UTF_8);
		}
		return "SLNode{key=" + keyString + ", value=" + valueString + ", isDeleted=" + deleted + "}";
	}

}
