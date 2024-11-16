package org.apache.flamingo.sstable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Getter;
import org.apache.flamingo.utils.Pair;
import org.apache.flamingo.utils.StringUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Level Meta Information.
 */
public class LevelMetaInfo {

    @Getter
    private final int level;

    // We should try to find a data structure
    // that supports the order of adding records and can quickly locate them
    // Now we choose LinkedHashMap
    private final Map<String, SSTableInfo> tables;

    private final ReentrantReadWriteLock locker;

    private final ObjectMapper objectMapper;

    public LevelMetaInfo(int level) {
        this.level = level;
        this.tables = new LinkedHashMap<>();
        this.locker = new ReentrantReadWriteLock();
        this.objectMapper = new ObjectMapper();
    }

    public int size() {
        locker.readLock().lock();
        try {
            return tables.size();
        } finally {
            locker.readLock().unlock();
        }
    }

    public List<SSTableInfo> chooseNeedCompactTable() {
        locker.readLock().lock();
        try {
            Set<String> keySet = tables.keySet();
            List<SSTableInfo> result = new ArrayList<>();
            if(!keySet.isEmpty()) {
                keySet.stream().findFirst().ifPresent(
                        s -> {
                            SSTableInfo table = tables.get(s);
                            result.add(table);
                        }
                );
            }
            return result;
        } finally {
            locker.readLock().unlock();
        }
    }

    public void addTable(SSTableInfo table) {
        locker.writeLock().lock();
        try {
            String tableId = table.getId();
            SSTableInfo sst = tables.get(tableId);
            if (sst == null) {
                tables.put(tableId, table);
            } else {
                throw new RuntimeException("table already exists: " + tableId);
            }
        } finally {
            locker.writeLock().unlock();
        }
    }

    public void deleteTable(SSTableInfo table) {
        locker.writeLock().lock();
        try {
            // TODO: 考虑此处的删除操作移到锁外执行以便于减小持有锁的时间
            tables.remove(table.getId());
        } finally {
            locker.writeLock().unlock();
        }
    }

    public void deleteTable(List<SSTableInfo> delTables) {
        locker.writeLock().lock();
        try {
            delTables.forEach(table -> {
                String tableId = table.getId();
                if (tables.containsKey(tableId)) {
                    tables.remove(tableId);
                    // TODO: 考虑此处的删除操作移到锁外执行以便于减小持有锁的时间
                    table.delete();
                }
            });
        } finally {
            locker.writeLock().unlock();
        }
    }

    public List<SSTableInfo> getOverlapTables(SSTableInfo table) {
        locker.readLock().lock();
        ArrayList<SSTableInfo> res = new ArrayList<>();
        try {
            tables.values().forEach(info -> {
                if (hasOverlap(info, table)) {
                    res.add(info);
                }
            });
        } finally {
            locker.readLock().unlock();
        }
        return res;
    }

    private boolean hasOverlap(SSTableInfo newTable, SSTableInfo oldTable) {
        byte[] newMin = newTable.getMetaInfo().getMinimumValue();
        byte[] oldMin = oldTable.getMetaInfo().getMinimumValue();
        byte[] newMax = newTable.getMetaInfo().getMaximumValue();
        byte[] oldMax = oldTable.getMetaInfo().getMaximumValue();
        return StringUtil.compareByteArrays(newMax, oldMin) >= 0 && StringUtil.compareByteArrays(oldMax, newMin) >= 0;
    }

    /**
     * According to the order of addition, we should prioritize traversing from back to front.
     *
     * @param key  Search key.
     * @return f0: value
     *         f1: Did you find the key in this layer ?
     */
    public Pair<byte[], Boolean> search(byte[] key) {
        locker.readLock().lock();
        try {
            if(tables.isEmpty()) {
                return Pair.of(null, false);
            }
            // If the data at layer 0 cannot be found, continue searching downwards
            if(level == 0) {
                for (SSTableInfo table : reverse()) {
                    Pair<byte[], Boolean> pair = table.search(key);
                    if (pair.getF1()) {
                        return pair;
                    }
                }
                return Pair.of(null, false);
            }
            for (SSTableInfo table : reverse()) {
                byte[] minimumValue = table.getMetaInfo().getMinimumValue();
                byte[] maximumValue = table.getMetaInfo().getMaximumValue();
                if (StringUtil.compareByteArrays(minimumValue, key) <= 0
                        && StringUtil.compareByteArrays(maximumValue, key) >= 0) {
                    Pair<byte[], Boolean> searchPair = table.search(key);
                    if (searchPair.getF1()) {
                        return searchPair;
                    }
                    break;
                }
            }
            return Pair.of(null, false);
        } finally {
            locker.readLock().unlock();
        }
    }

    private List<SSTableInfo> reverse() {
        ArrayList<SSTableInfo> res = new ArrayList<>(tables.values());
        Collections.reverse(res);
        return res;
    }

    public ArrayNode serialize() {
        locker.readLock().lock();
        ArrayNode arrayNode = objectMapper.createArrayNode();
        try {
            if(tables.isEmpty()) {
                return null;
            } else {
                for (SSTableInfo table : reverse()) {
                    JsonNode jsonNode = SSTableInfo.toJson(table);
                    if(jsonNode != null) {
                        arrayNode.add(jsonNode);
                    }
                }
                return arrayNode;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            locker.readLock().unlock();
        }
    }

    public String serializeToString() throws JsonProcessingException {
        locker.readLock().lock();
        try {
            if(tables.isEmpty()) {
                return null;
            } else {
                return objectMapper.writeValueAsString(tables);
            }
        } finally {
            locker.readLock().unlock();
        }
    }

}
