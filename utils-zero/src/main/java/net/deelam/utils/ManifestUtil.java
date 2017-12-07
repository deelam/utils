package net.deelam.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ManifestUtil {
  //static Logger log=Logger.getLogger(ManifestUtil.class.getName());
  
  public static void main(String[] args) throws Exception {
    //JavaLoggingUtil.configureJUL("%1$tH:%1$tM:%1$tS [%4$s] %5$s%6$s%n");
    
//    Path p = Paths.get("lib/commons-io-2.4.jar");
//    System.out.println(Paths.get("lib").relativize(p));
//    log.severe("hiihihihii");
    if(args.length==0)
      copyClasspathJars("target/adidis-install.jar", "libs3", false);
    else
      copyClasspathJars(args[0], args[1], true);
  }
 
  public static void copyClasspathJars(String jarFile, String targetDir, boolean symlink) throws IOException {
    String cp = getManifestProperty(jarFile, "Class-Path");
    File destDir=new File(targetDir);
    destDir.mkdirs();
    Arrays.stream(cp.split(" ")).forEach(filename->{
      if(filename.equals("."))
        return;
      if(!filename.endsWith(".jar")){
        log.info("Skipping non-jar file: "+filename);
        return;
      }
      File file = new File(filename);
      if(file.exists()){
        log.info("Linking to "+file+" in "+destDir);
        try {
          //FileUtils.copyFileToDirectory(file, destDir);
          Path srcPath = Paths.get(filename);
          if(symlink)
            Files.createSymbolicLink(
                Paths.get(targetDir, srcPath.getFileName().toString()), 
                Paths.get(targetDir).relativize(srcPath));
          else
            Files.createLink(
                Paths.get(targetDir, srcPath.getFileName().toString()), 
                srcPath);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }else{
        log.warn("Skipping non-existing file: "+file);
      }
    });
  }
  
  public static List<String> getClasspathJars(String jarFile) throws IOException {
    String cp = getManifestProperty(jarFile, "Class-Path");
    if(cp==null)
      throw new RuntimeException("Class-Path missing from MANIFEST.MF of jar: "+jarFile);
    List<String> list=new ArrayList<>();
    Arrays.stream(cp.split(" ")).forEach(filename->{
      if(filename.equals("."))
        return;
      File file = new File(filename);
      if(file.exists()){
        list.add(filename);
      }else{
        log.warn("Skipping non-existing file: "+file);
      }
    });
    return list;
  }
  
  public static String getManifestProperty(String jarFilename, String manifestKey) throws IOException {
    try(JarFile file = new JarFile(jarFilename)){
      ZipEntry entry = file.getEntry("META-INF/MANIFEST.MF");
      InputStream is = file.getInputStream(entry);
      if (is == null) {
        return "(No META-INF/MANIFEST.MF file)";
      }
      return readManifestProperty(is, manifestKey);
    }
  }

  public static String getManifestProperty(ClassLoader clContainingManifest, String manifestKey) throws IOException {
    // Check Manifest file for generated build number
    InputStream is = clContainingManifest.getResourceAsStream("META-INF/MANIFEST.MF");
    return readManifestProperty(is, manifestKey);
  }

  private static String readManifestProperty(InputStream is, String manifestKey) throws IOException {
    Manifest mf = new Manifest();
    if (is == null) {
      return "(No META-INF/MANIFEST.MF file)";
    }
    mf.read(is);
    return mf.getMainAttributes().getValue(manifestKey);
  }

}
