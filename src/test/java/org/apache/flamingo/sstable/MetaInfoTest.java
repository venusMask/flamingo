package org.apache.flamingo.sstable;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaInfoTest extends TestCase {

    private final String metaFileLocation = "data/meta-info.json";

    public void testParallelOperation() throws InterruptedException, IOException {
        DiskMetaInfo metaInfo = new DiskMetaInfo(metaFileLocation);
        CountDownLatch downLatch = new CountDownLatch(10);
        AtomicInteger id = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                for (int j = 0; j < 10; j++) {
                    SSTableInfo ssTableInfo = SSTableTest.generateEmptySSTable();
                    ssTableInfo.setId(String.valueOf(id.getAndIncrement()));
                    metaInfo.addTable(ssTableInfo);
                    downLatch.countDown();
                }
            }).start();
        }
        downLatch.await();
        metaInfo.serialize();
    }


}
