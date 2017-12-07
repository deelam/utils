package net.deelam.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.LogManager;

public class JavaLoggingUtil {
  
    public static void configureJUL() throws IOException {
      configureJUL("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS [%4$s] %2$s: %5$s%6$s%n");
      //configureJUL("%1$tH:%1$tM:%1$tS [%4$s] %5$s%6$s%n");
    }
    
    public static void configureJUL(String format) throws IOException {
      final String julConfigStr=""
          + ".level=INFO\n"
          + "handlers=java.util.logging.ConsoleHandler\n"
          + "java.util.logging.ConsoleHandler.level=FINE\n"
          + "java.util.logging.ConsoleHandler.formatter=java.util.logging.SimpleFormatter\n"
          + "java.util.logging.SimpleFormatter.format="+format+"\n"
          + "";
      //System.out.println(julConfigStr);
      InputStream is = new ByteArrayInputStream(julConfigStr.getBytes(StandardCharsets.UTF_8));
      System.out.println(JavaLoggingUtil.class+": Enabling JUL: "+format);
      LogManager.getLogManager().readConfiguration(is);
    }
    
    public static void disableJUL() {
      System.out.println(JavaLoggingUtil.class+": Disabling JUL");
      LogManager.getLogManager().reset();
    }    
}

