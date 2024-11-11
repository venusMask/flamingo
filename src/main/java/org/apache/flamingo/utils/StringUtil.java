package org.apache.flamingo.utils;

import java.nio.charset.StandardCharsets;

/**
 * @Author venus
 * @Date 2024/11/10
 * @Version 1.0
 */
public class StringUtil {

    public static byte[] fromString(String s) {
        if(s == null) {
            return null;
        }
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String fromBytes(byte[] bytes) {
        if(bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
