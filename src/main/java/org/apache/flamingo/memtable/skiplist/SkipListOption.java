package org.apache.flamingo.memtable.skiplist;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SkipListOption {

	private int maxLevel = 32;

	private double probability = 0.5f;

}
