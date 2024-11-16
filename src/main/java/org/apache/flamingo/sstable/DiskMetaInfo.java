package org.apache.flamingo.sstable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.utils.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

@Slf4j
public class DiskMetaInfo {

    private final Map<Integer, LevelMetaInfo> metaInfo = new HashMap<>();

    private final ObjectMapper mapper = new ObjectMapper();

    private final String metaFileLocation;

    private final Compact compact;

    private final int maxLevel;

    public DiskMetaInfo(String filePath) {
        this.metaFileLocation = filePath;
        this.maxLevel = Integer.parseInt(Options.MaxLevel.getValue());
        this.compact = new Compact();
        initEmptyLevel();
    }

    private void initEmptyLevel() {
        for (int level = 0; level < maxLevel; level++) {
            metaInfo.put(level, new LevelMetaInfo(level));
        }
    }

    public void addTable(SSTableInfo table) {
        int level = table.getLevel();
        LevelMetaInfo levelMetaInfo = metaInfo.get(level);
        if(levelMetaInfo == null) {
            log.debug("Error level: {}", level);
        }
        levelMetaInfo.addTable(table);
        if(levelMetaInfo.size() > maxLevelSize(level)) {
            // TODO Compact
            log.debug("Begin Compact ...");
        }
    }

    /**
     * Merge data from the level layer
     *
     * @param level level number
     */
    private void compact(int level) {
        LevelMetaInfo levelMetaInfo = metaInfo.get(level);
        List<SSTableInfo> upperLevel = levelMetaInfo.chooseNeedCompactTable();
        List<SSTableInfo> lowerLevel = getOverlapTables(upperLevel, 1);
        compact.majorCompact(upperLevel, lowerLevel);
    }

    private List<SSTableInfo> getOverlapTables(List<SSTableInfo> upperTables, int nextLevel) {
        LevelMetaInfo levelMetaInfo = metaInfo.get(nextLevel);
        ArrayList<SSTableInfo> lowerTable = new ArrayList<>();
        upperTables.forEach(table -> {
            List<SSTableInfo> overlapTables = levelMetaInfo.getOverlapTables(table);
            if(!overlapTables.isEmpty()) {
                lowerTable.addAll(overlapTables);
            }
        });
        return lowerTable;
    }

    public int maxLevelSize(int level) {
        return 2;
    }

    private byte[] search(byte[] key) {
        for (int level = 0; level < maxLevel; level++) {
            LevelMetaInfo levelMetaInfo = metaInfo.get(level);
            Pair<byte[], Boolean> pair = levelMetaInfo.search(key);
            if(pair.getF1()) {
                return pair.getF0();
            }
        }
        return null;
    }

    public void serialize() throws IOException {
        ObjectNode node = mapper.createObjectNode();
        for (int level = 0; level < maxLevel; level++) {
            LevelMetaInfo levelMetaInfo = metaInfo.get(level);
            ArrayNode serialized = levelMetaInfo.serialize();
            if(serialized != null) {
                node.set(String.valueOf(level), serialized);
            }
        }
        String prettyString = mapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(node);
        try (FileWriter fileWriter = new FileWriter(metaFileLocation)) {
            fileWriter.write(prettyString);
        }
    }
}
