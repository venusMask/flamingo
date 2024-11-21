package org.apache.flamingo.memtable;

import junit.framework.TestCase;
import org.apache.flamingo.utils.GeneratorDataUtil;
import org.apache.flamingo.utils.StringUtil;

import java.io.IOException;

public class MemTableTest extends TestCase {

    public void testAdd() {
        try (MemoryTable memoryTable = new MemoryTable()) {
            String key = GeneratorDataUtil.generateRandomString();
            String value = GeneratorDataUtil.generateRandomString();
            memoryTable.add(
                    StringUtil.fromString(key),
                    StringUtil.fromString(value)
            );
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }


}
