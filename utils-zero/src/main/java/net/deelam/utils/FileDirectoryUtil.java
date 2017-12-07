package net.deelam.utils;

import java.io.File;
import java.io.FileFilter;

public final class FileDirectoryUtil {
  private static FileFilter directoryFilter = (File dir) -> dir.isDirectory();

  public static File findFirstDirectoryWithPrefix(final File parentDir, final String[] dirPrefixes) {
    final File[] fileList = parentDir.listFiles(directoryFilter);
    for (String prefix : dirPrefixes) {
      for (File f : fileList)
        if (f.getName().startsWith(prefix))
          return f;
    }
    return null;
  }
}
