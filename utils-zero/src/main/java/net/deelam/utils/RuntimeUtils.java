package net.deelam.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class RuntimeUtils {
  public static Process exec(String logStringPrefix, String... callAndArgs) throws IOException {
    log.info("Starting new process to run: {}", Arrays.toString(callAndArgs));
     Process p = Runtime.getRuntime().exec(callAndArgs);

    startOutputGrabberThread(logStringPrefix, p, p.getInputStream(), OutputType.INFO);
    startOutputGrabberThread(logStringPrefix, p, p.getErrorStream(), OutputType.WARN);
    return p;
  }

  enum OutputType {
    DEBUG, INFO, WARN
  }

  private static Thread startOutputGrabberThread(String logStringPrefix, Process p,
      InputStream inputStream, OutputType outType) {
    Thread t = new Thread(() -> {
      log.info("Redirecting process '{}' output to logtype={}", logStringPrefix, outType);
      try {
        String s;
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(inputStream));
        while ((s = stdInput.readLine()) != null) {
          switch (outType) {
            case DEBUG:
              log.debug("{}: {}", logStringPrefix, s);
              break;
            case INFO:
              log.info("{}: {}", logStringPrefix, s);
              break;
            case WARN:
              log.warn("{}: {}", logStringPrefix, s);
              break;
          }
        }
      } catch (IOException e) {
        log.error("While running {}:", logStringPrefix, e);
      }
      log.info("Done redirecting process '{}' output to logtype={}", logStringPrefix, outType);
    }, "mySubprocessOutput-" + outType + "-" + logStringPrefix);
    t.setDaemon(true);
    t.start();
    return t;
  }
}
