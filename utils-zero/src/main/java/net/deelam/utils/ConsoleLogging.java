package net.deelam.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access=AccessLevel.PRIVATE)
public class ConsoleLogging {

  public static Logger createSlf4jLogger(Class<?> clazz) {
    return createSlf4jLogger("console." + clazz.getSimpleName());
  }
  
  public static Logger createSlf4jLogger(String loggerName) {
    return LoggerFactory.getLogger(loggerName);
  }

}
