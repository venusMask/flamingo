package org.apache.flamingo.file;

import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.core.IDAssign;
import org.apache.flamingo.options.Options;
import org.apache.flamingo.sstable.SSTableInfo;
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

	public static String getSSTFileName() {
		return Options.DataDir.getValue() + File.separator + SSTableInfo.SSTABLE + IDAssign.getSSTNextID() + ".sst";
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
			log.debug("Deleted " + filePath);
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

	public static boolean checkFileExists(String file, boolean throwException) {
		Path path = Paths.get(file);
		if (Files.exists(path)) {
			return true;
		}
		if (throwException) {
			throw new RuntimeException(file + " does not exist");
		}
		return false;
	}

	public static void checkFileExistsOrCreate(String file) {
		Path path = Paths.get(file);
		if (!Files.exists(path)) {
			try {
				Files.createDirectories(path.getParent());
				Files.createFile(path);
			}
			catch (IOException e) {
				throw new RuntimeException("Could not create directory or file: " + path, e);
			}
		}
	}

	public static void deleteFile(String file) {
		log.debug("Deleting " + file);
		Path path = Paths.get(file);
		try {
			boolean flag = Files.deleteIfExists(path);
			if (!flag) {
				throw new RuntimeException("Could not delete file: " + path);
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Could not delete file: " + path, e);
		}
	}

	public static void deleteDirectory(Path dir) throws IOException {
		if (Files.exists(dir)) {
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
				for (Path path : stream) {
					if (Files.isDirectory(path)) {
						deleteDirectory(path); // 递归删除子目录
					} else {
						Files.delete(path); // 删除文件
					}
				}
			}
			Files.delete(dir); // 删除当前目录
		}
	}

}
