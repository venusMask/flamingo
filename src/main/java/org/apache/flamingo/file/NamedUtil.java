package org.apache.flamingo.file;

import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.utils.Pair;
import org.apache.flamingo.writer.VLogWriter;

public class NamedUtil {

	public static String getDataDir() {
		return Options.DataDir.getValue();
	}

	public static String getValueDir() {
		return getDataDir() + "/value";
	}

	public static String getKeyDir() {
		return getDataDir() + "/key";
	}

	public static String getMetaDir() {
		return getDataDir() + "/meta/meta.json";
	}

	public static Pair<String, Long> getValueFilePath() {
		String id = IDAssign.getWALNextID();
		String fileName = getValueDir() + "/" + String.format(VLogWriter.ACTIVE, id);
		return Pair.of(fileName, Long.parseLong(id));
	}

	public static Pair<String, Long> getKeyFilePath() {
		String id = IDAssign.getSSTNextID();
		String fileName = getKeyDir() + "/" + id + ".sst";
		return Pair.of(fileName, Long.parseLong(id));
	}

}
