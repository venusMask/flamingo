package org.apache.flamingo.utils;

import java.nio.charset.StandardCharsets;

/**
 * @Author venus
 * @Date 2024/11/10
 * @Version 1.0
 */
public class StringUtil {

	public static byte[] fromString(String s) {
		if (s == null) {
			return null;
		}
		return s.getBytes(StandardCharsets.UTF_8);
	}

	public static String fromBytes(byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		return new String(bytes, StandardCharsets.UTF_8);
	}

	public static int compareByteArrays(byte[] array1, byte[] array2) {
		int minLength = Math.min(array1.length, array2.length);
		for (int i = 0; i < minLength; i++) {
			if (array1[i] != array2[i]) {
				return Byte.compare(array1[i], array2[i]);
			}
		}
		// If we reach here, the arrays are equal up to the length of the shorter one.
		// The longer array is considered greater.
		return Integer.compare(array1.length, array2.length);
	}

	public static byte fromBool(boolean flag) {
		return flag ? (byte) 1 : (byte) 0;
	}

	public static boolean fromByte(byte b) {
		return b == (byte) 1;
	}

}
