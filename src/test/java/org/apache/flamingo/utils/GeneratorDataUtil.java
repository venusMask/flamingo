package org.apache.flamingo.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flamingo.options.Options;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;

/**
 * @Author venus
 * @Date 2024/11/11
 * @Version 1.0
 */
public class GeneratorDataUtil {

	private static final RandomStringUtils
			randomStringUtils = RandomStringUtils.secure();

	private static final Random random = new Random();

	private static final int maxLevel = Integer.parseInt(Options.MaxLevel.getValue());

	public static boolean generateBoolean() {
		return random.nextBoolean();
	}

	public static long generateLong() {
		return random.nextLong();
	}

	public static int generateRandomLevel() {
		return RandomUtils.secure().randomInt(0, maxLevel);
	}

	public static String generateRandomString() {
		return randomStringUtils.nextAlphabetic(1, 20);
	}

	public static String generateRandomString(int min, int max) {
		return randomStringUtils.nextAlphabetic(min, max);
	}

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
