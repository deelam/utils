package net.deelam.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PropertiesUtil {
  
  public static void loadProperties(String filename, Properties properties) throws FileNotFoundException, IOException {
    loadProperties(filename, properties, "  ");
  }
  
  public static void loadProperties(String filename, Properties properties, String indent) throws FileNotFoundException, IOException {
    File propFile=new File(filename);
    if (!propFile.exists()) { // then look in classpath
      //System.out.println("Searching for property file in classpath: "+filename);
      URL url = Runtime.class.getResource("/"+filename);
      if(url!=null)
        try {
          propFile=new File(url.toURI());
        } catch (URISyntaxException e) {
        }
    }
    if (propFile.exists()) {
      log.info(indent+"Loading property file: "+ propFile.getAbsolutePath());
      try (InputStreamReader reader = new InputStreamReader(new FileInputStream(propFile), "UTF-8")) {
        properties.load(reader);
      }
      String includedFiles=properties.getProperty("include");
      if (includedFiles != null){
        log.info(indent+"  Also loading included files: "+ includedFiles);
        for (String includedFile : includedFiles.split(",")) {
          Properties includedProps = new Properties();
          loadProperties(includedFile.trim(), includedProps, indent+"  ");
          // TODO: add an option to not override the 'include' property
          for (String key : includedProps.stringPropertyNames()){
            if(key.equals("include"))
              continue;
            String includedProp=includedProps.getProperty(key);
            if (includedProp!=null){
              if(properties.getProperty(key)==null){
                properties.put(key, includedProp);
              } else if(includedProp.equals(properties.getProperty(key)))
                log.info(indent+"Included "+key+"="+includedProp+
                    " is same as '"+properties.getProperty(key)+"'");
              else
                log.info(indent+"Ignoring included "+key+"="+includedProp+
                    ", using value '"+properties.getProperty(key)+"' instead");
            }
          }
        }
      }
    } else {
      throw new FileNotFoundException("Property file doesn't exist: " + propFile.getAbsolutePath());
    }
  }
  
}

