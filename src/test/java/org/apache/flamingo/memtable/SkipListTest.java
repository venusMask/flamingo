package org.apache.flamingo.memtable;

import junit.framework.TestCase;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flamingo.lsm.FlamingoLSM;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.flamingo.memtable.DefaultMemTable.readByteBuffer;

/**
 * @Author venus
 * @Date 2024/11/5
 * @Version 1.0
 */
public class SkipListTest extends TestCase {

    public FlamingoLSM lsm = new FlamingoLSM();

    public byte[] fromString(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static ArrayList<byte[]> testData(int len) {
        RandomStringUtils randomStringUtils = RandomStringUtils.secure();
        ArrayList<byte[]> arrayList = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            arrayList.add(randomStringUtils.nextAlphabetic(5, 20).getBytes(StandardCharsets.UTF_8));
        }
        return arrayList;
    }

    public void testSeq() {

    }

    public long testDataLen(DefaultMemTable skipList, int len) {
        ArrayList<byte[]> keyList = testData(len);
        ArrayList<byte[]> valueList = testData(len);
        long start = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
            skipList.add(keyList.get(i), valueList.get(i));
        }
        return System.currentTimeMillis() - start;
    }

    public void testDefaultSkipList() {
        DefaultMemTable defaultSkipList = new DefaultMemTable(lsm);
        defaultSkipList.add(fromString("a"), fromString("a"));
        defaultSkipList.add(fromString("b"), fromString("b"));
        defaultSkipList.add(fromString("c"), fromString("c"));
        defaultSkipList.add(fromString("a"), fromString("d"));
        ConcurrentSkipListMap<byte[], byte[]> memTable = defaultSkipList.getMemTable();
        Set<Map.Entry<byte[], byte[]>> entrySet = memTable.entrySet();
        entrySet.forEach(entry -> {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            String keyString = new String(key);
            String valueString = new String(value);
            System.out.println("key: " + keyString + ", value: " + valueString);
        });
    }

    public void testDefaultSkipListRate() {
        try (DefaultMemTable memTable = new DefaultMemTable(lsm)) {
            long time4 = testDataLen(memTable, 100000);
            System.out.println(time4);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testReadFile() {
        String filePath = "/Users/dzh/software/java/projects/flamingo/data/sstable_1.sst";
        try {
            FileInputStream fileInputStream = new FileInputStream(filePath);
            FileChannel readChannel = fileInputStream.getChannel();
            int available = fileInputStream.available();
            ByteBuffer byteBuffer = ByteBuffer.allocate(available);
            readChannel.read(byteBuffer);
            byteBuffer.flip();
            while (true) {
                byte[] keyByte = readByteBuffer(byteBuffer);
                if(keyByte == null) {
                    break;
                }
                byte[] valueByte = readByteBuffer(byteBuffer);
                String keyStr = new String(keyByte);
                String valueStr = new String(valueByte);
                System.out.println(keyStr + " <-> " + valueStr);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
