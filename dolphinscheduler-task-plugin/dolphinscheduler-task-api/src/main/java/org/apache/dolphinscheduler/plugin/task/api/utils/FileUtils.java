package org.apache.dolphinscheduler.plugin.task.api.utils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.RWXR_XR_X;

@Slf4j
public class FileUtils {

    private static final FileAttribute<Set<PosixFilePermission>> PERMISSION_755 =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(RWXR_XR_X));

    /**
     * Create a file with '755'.
     */
    public static void createFileWith755(@NonNull Path path) throws IOException {
        if (OSUtils.isWindows()) {
            Files.createFile(path);
        } else {
            Files.createFile(path, PERMISSION_755);
        }
    }
}
