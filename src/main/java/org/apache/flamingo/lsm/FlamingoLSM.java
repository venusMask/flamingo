package org.apache.flamingo.lsm;

import org.apache.flamingo.memtable.DefaultMemTable;
import org.apache.flamingo.options.Options;

public class FlamingoLSM {

    private DefaultMemTable memTable;

    private final Options options = Options.getInstance();

    private final int memTableSize;

    public FlamingoLSM() {
        this.memTableSize = Integer.parseInt(Options.MEM_SIZE.getValue());
        memTable = new DefaultMemTable();
        initMeta();
    }

    public void initMeta() {

    }

    /**
     * MemTable的大小超过阈值的时候将当前memTable设置为不可变对象, 然后新构建一个MemTable接受新的请求.
     */
    public boolean add(byte[] key, byte[] value) {
        memTable.add(key, value);
        if(memTable.size() > memTableSize) {
            memTable.switchState();
            memTable = new DefaultMemTable();
        }
        return true;
    }

    public boolean delete(byte[] key) {
        memTable.delete(key);
        return true;
    }

    public byte[] search(byte[] key) {
        return memTable.search(key);
    }

}
