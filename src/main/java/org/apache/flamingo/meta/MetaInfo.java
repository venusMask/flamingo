package org.apache.flamingo.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.core.Context;
import org.apache.flamingo.file.NamedUtil;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.utils.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

@Slf4j
public class MetaInfo {

    private final Map<Integer, LevelMetaInfo> metaInfo = new HashMap<>();

    private final ObjectMapper mapper = Context.getInstance().getObjectMapper();

    private final String metaFileLocation;

    @Getter
    private final Compact compact;

    private final int maxLevel;

    public MetaInfo(String filePath) {
        this.metaFileLocation = filePath;
        this.maxLevel = Integer.parseInt(Options.MaxLevel.getValue());
        this.compact = new Compact(this);
        initEmptyLevel();
    }

    public MetaInfo() {
        this(NamedUtil.getMetaDir());
    }

    private void initEmptyLevel() {
        for (int level = 0; level < maxLevel; level++) {
            metaInfo.put(level, new LevelMetaInfo(level));
        }
    }

    /**
     * Add entry point for SST information.
     */
    public void addTable(SSTMetaInfo table) {
        checkSST(table);
        int level = table.getLevel();
        LevelMetaInfo levelMetaInfo = metaInfo.get(level);
        levelMetaInfo.addTable(table);
        if(levelMetaInfo.size() > maxLevelSize(level) && level != maxLevel - 1) {
            compact(level);
        }
    }

    public void removeTable(List<SSTMetaInfo> tables){
        if(tables != null && !tables.isEmpty()) {
            int level = tables.get(0).getLevel();
            LevelMetaInfo levelMetaInfo = metaInfo.get(level);
            levelMetaInfo.deleteTable(tables);
        }
        serialize();
    }

    /**
     * Merge data from the level layer
     *
     * @param level level number
     */
    private void compact(int level) {
        LevelMetaInfo levelMetaInfo = metaInfo.get(level);
        List<SSTMetaInfo> upperLevel = levelMetaInfo.chooseNeedCompactTable();
        if(upperLevel.isEmpty()) {
            serialize();
            throw new NullPointerException("Upper Level is Empty!");
        }
        List<SSTMetaInfo> lowerLevel = getOverlapTables(upperLevel, level + 1);
        compact.majorCompact(upperLevel, lowerLevel);
    }

    private List<SSTMetaInfo> getOverlapTables(List<SSTMetaInfo> upperTables, int nextLevel) {
        LevelMetaInfo levelMetaInfo = metaInfo.get(nextLevel);
        ArrayList<SSTMetaInfo> lowerTable = new ArrayList<>();
        upperTables.forEach(table -> {
            List<SSTMetaInfo> overlapTables = levelMetaInfo.getOverlapTables(table);
            if(!overlapTables.isEmpty()) {
                lowerTable.addAll(overlapTables);
            }
        });
        return lowerTable;
    }

    /**
     * 计算每层sst的阈值
     */
    public int maxLevelSize(int level) {
        return 2;
    }

    public byte[] search(byte[] key) {
        for (int level = 0; level < maxLevel; level++) {
            LevelMetaInfo levelMetaInfo = metaInfo.get(level);
            Pair<byte[], Boolean> pair = levelMetaInfo.search(key);
            if(pair.getF1()) {
                return pair.getF0();
            }
        }
        return null;
    }

    public void serialize() {
        ObjectNode node = mapper.createObjectNode();
        for (int level = 0; level < maxLevel; level++) {
            LevelMetaInfo levelMetaInfo = metaInfo.get(level);
            ArrayNode serialized = levelMetaInfo.serialize();
            if(serialized != null) {
                node.set(String.valueOf(level), serialized);
            }
        }
        try (FileWriter fileWriter = new FileWriter(metaFileLocation)) {
            String prettyString = mapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(node);
            fileWriter.write(prettyString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkSST(SSTMetaInfo sst) {
        if(sst == null || sst.getLevel() >= maxLevel) {
            throw new RuntimeException("Error sst be added!, info: " + (sst == null ? "null" : sst));
        }
    }
}
