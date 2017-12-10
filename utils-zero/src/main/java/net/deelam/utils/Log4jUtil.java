package net.deelam.utils;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

/**
 * See http://logging.apache.org/log4j/1.2/manual.html#Default_Initialization_Procedure
 */
public class Log4jUtil {

  private static final String LOG4J_CONFIGURATION_KEY = "log4j.configuration";

  public static void loadXml() {
    File file = new File("log4j.xml");
    if (System.getProperty(LOG4J_CONFIGURATION_KEY) == null && file.exists()) {
      // set a default if file exist
      System.out.println("System.setProperty: "+LOG4J_CONFIGURATION_KEY+"="+file.toURI().toString());
      System.setProperty(LOG4J_CONFIGURATION_KEY, file.toURI().toString());
      System.out.println("Found log4j.xml file in current directory.  Set " + LOG4J_CONFIGURATION_KEY + "="
          + System.getProperty(LOG4J_CONFIGURATION_KEY));
    } else {
      checkForLog4jFile();
    }
  }

  private static void checkForLog4jFile() {
    String logConfigFile = System.getProperty(LOG4J_CONFIGURATION_KEY);
    if (logConfigFile != null) { // if property set, then check that file can be found
      //System.out.println("  Checking that System property: log4j.configurationFile=" + logConfigFile+" is in classpath");
      {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL foundLogConfigFile = cl.getResource(logConfigFile);
        if (foundLogConfigFile == null) {
          URL[] urls = ((URLClassLoader) cl).getURLs();
          System.err.println("  !!! Could not find logging config file " + logConfigFile
              + " in classpath: " + Arrays.toString(urls));
          System.err.println("Remember to add path to log4j.xml file to classpath!");
        } else {
          System.out.println("  Found in classpath: " + foundLogConfigFile);
        }
      }
    }
  }
}
