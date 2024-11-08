package org.apache.flamingo.file;

import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.sstable.SSTable;
import org.apache.flamingo.wal.WALWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class FileUtil {

	public static String getSSTFilePath() {
		return Options.DataDir.getValue() + File.separator + SSTable.SSTABLE + IDAssign.getSSTNextID() + ".sst";
	}

	public static String getWalActiveName() {
		return WALWriter.ACTIVE + IDAssign.getWALNextID() + ".wal";
	}

	public static String getMetaInfoPath() {
		return getDataDirFilePath("meta_info" + ".meta");
	}

	public static String getDataDirPath() {
		return Options.DataDir.getValue();
	}

	public static String getDataDirFilePath(String fileName) {
		return getDataDirPath() + File.separator + fileName;
	}

	public static boolean deleteDataDirFile(String fileName) {
		String filePath = getDataDirFilePath(fileName);
		try {
			Files.deleteIfExists(Paths.get(filePath));
			return true;
		}
		catch (IOException e) {
			return false;
		}
	}

	public static boolean renameDataDirFile(String oldFileName, String newFileName) {
		File file = new File(getDataDirFilePath(oldFileName));
		return file.renameTo(new File(getDataDirFilePath(newFileName)));
	}

	public static int getMaxOrder(String dir, String regex) {
		int maxNum = 0;
		Pattern pattern = Pattern.compile(regex);
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
			for (Path entry : stream) {
				Matcher matcher = pattern.matcher(entry.getFileName().toString());
				if (matcher.matches()) {
					int num = Integer.parseInt(matcher.group(1));
					if (num > maxNum) {
						maxNum = num;
					}
				}
			}
		}
		catch (IOException e) {
			System.err.println("读取目录时发生错误: " + e.getMessage());
		}
		return maxNum + 1;
	}

	public static int getMaxOrder(String regex) {
		return getMaxOrder(getDataDirPath(), regex);
	}

}
