package org.apache.flamingo.utils;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author venus
 * @Date 2024/11/6
 * @Version 1.0
 */
public class FileUtil {

    /**
     * 遍历dir目录下的所有文件, 然后将每行中包含matchContent的行替换成replaceContent
     * @param dir               目录
     * @param matchContent      需要替换的行包含的内容
     * @param replaceContent    需要被替换的内容
     */
    public static void replaceContent(String dir, String matchContent, String replaceContent) {
        Path directory = Paths.get(dir);
        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            System.out.println("指定的目录不存在或不是一个有效的目录: " + dir);
            return;
        }

        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (Files.isRegularFile(file)) {
                        List<String> lines = Files.readAllLines(file);
                        List<String> updatedLines = new ArrayList<>();

                        for (String line : lines) {
                            if (line.contains(matchContent)) {
                                updatedLines.add(line.replace(matchContent, replaceContent));
                            } else {
                                updatedLines.add(line);
                            }
                        }

                        if (!updatedLines.equals(lines)) {
                            Files.write(file, updatedLines);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    System.err.println("无法访问文件: " + file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            System.err.println("遍历目录时发生错误: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String dir = "/Users/dzh/software/apollo/lightcone/manager/src/main/java/org/lightcone/manager/standard/controller";
        String matchContent = "com.springboot.cloud.dataStandard.vo";
        String replaceContent = "org.lightcone.repository.standard.vo";
        replaceContent(dir, matchContent, replaceContent);
    }

}
