package org.apache.flamingo.lsm;

import lombok.Getter;
import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.file.FileUtil;
import org.apache.flamingo.memtable.DefaultMemTable;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.sstable.SSTMetadata;

@Getter
public class FlamingoLSM {

    private DefaultMemTable memTable;

    private final Options options = Options.getInstance();

    private final int memTableSize;

    private final SSTMetadata sstMetadata;

    public FlamingoLSM() {
        this.memTableSize = Integer.parseInt(Options.MemTableThresholdSize.getValue());
        memTable = new DefaultMemTable(this);
        sstMetadata = new SSTMetadata();
        initMeta();
    }

    public void initMeta() {
        String sstRegex = "sstable_(\\d+)\\.sst";
        IDAssign.initSSTAssign(FileUtil.getMaxOrder(sstRegex));
        String walRegex = "wal_active_(\\d+)\\.wal";
        IDAssign.initWALAssign(FileUtil.getMaxOrder(walRegex));
    }

    /**
     * MemTable的大小超过阈值的时候将当前memTable设置为不可变对象, 然后新构建一个MemTable接受新的请求.
     */
    public boolean add(byte[] key, byte[] value) {
        memTable.add(key, value);
        if(memTable.size() > memTableSize) {
            memTable.switchState();
            memTable = new DefaultMemTable(this);
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
