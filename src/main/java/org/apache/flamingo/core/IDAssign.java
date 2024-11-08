package org.apache.flamingo.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * ID assigner, responsible for assigning id to ssTable and WalWriter
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

    public static void initSSTAssign(int initValue) {
        SSTAssign.set(initValue);
    }

    public static void initWALAssign(int initValue) {
        WALAssign.set(initValue);
    }

}
