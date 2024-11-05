package org.apache.flamingo.memtable;

import org.apache.flamingo.options.Options;
import org.apache.flamingo.wal.WALWriter;

/**
 * @Author venus
 * @Date 2024/11/5
 * @Version 1.0
 */
public interface MemTable {

    enum MemTableState {

        Active(1),
        Dead(2)
        ;

        public final int state;

        MemTableState(int state) {
            this.state = state;
        }
    }

    void add(byte[] key, byte[] value);

    void delete(byte[] key);

    byte[] search(byte[] key);

    Options getOptions();

    WALWriter getWalWriter();
}
