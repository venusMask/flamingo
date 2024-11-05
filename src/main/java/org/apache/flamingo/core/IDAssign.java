package org.apache.flamingo.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * ID分配器, 负责给ssTable和WalWriter分配数据
 */
public class IDAssign {

    private static final AtomicInteger SSTAssign = new AtomicInteger(0);

    private static final AtomicInteger WALAssign = new AtomicInteger(0);

    public static String getSSTNextID() {
        return String.valueOf(SSTAssign.getAndIncrement());
    }

    public static String getWALNextID() {
        return String.valueOf(WALAssign.getAndIncrement());
    }

}
