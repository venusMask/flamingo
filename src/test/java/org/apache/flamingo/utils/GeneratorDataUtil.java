package org.apache.flamingo.utils;

import org.apache.commons.lang3.RandomStringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * @Author venus
 * @Date 2024/11/11
 * @Version 1.0
 */
public class GeneratorDataUtil {

	private static final RandomStringUtils randomStringUtils = RandomStringUtils.secure();

	public static ArrayList<String> generateRandomStrings(int count, int minLength, int maxLength) {
		ArrayList<String> list = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			list.add(randomStringUtils.nextAlphabetic(minLength, maxLength));
		}
		return list;
	}

	public static ArrayList<byte[]> generateRandomBytes(int count, int minLength, int maxLength) {
		ArrayList<byte[]> list = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			list.add(randomStringUtils.nextAlphabetic(minLength, maxLength).getBytes(StandardCharsets.UTF_8));
		}
		return list;
	}

}
