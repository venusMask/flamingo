package org.apache.flamingo.lsm;

import junit.framework.TestCase;
import org.apache.flamingo.memtable.SkipListTest;

import java.util.ArrayList;

public class FlamingoLSMTest extends TestCase {

    public void testFlamingoLSM() throws InterruptedException {
        FlamingoLSM flamingoLSM = new FlamingoLSM();
        int len = 23;
        ArrayList<byte[]> keyList = SkipListTest.testData(len);
        ArrayList<byte[]> valueList = SkipListTest.testData(len);
        for (int i = 0; i < len; i++) {
            byte[] key = keyList.get(i);
            byte[] value = valueList.get(i);
            System.out.println(new String(key) + " <-> " + new String(value));
            flamingoLSM.add(key, value);
        }
    }

}
