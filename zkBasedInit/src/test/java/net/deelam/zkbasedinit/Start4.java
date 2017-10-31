package net.deelam.zkbasedinit;

import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ZkComponentStarter;
import net.deelam.zkbasedinit.ZkComponentStopper;
import net.deelam.zkbasedinit.ZkConfigPopulator;

@Slf4j
public class Start4 {

  public static void main(String[] args) throws Exception {
    // in JVM 1
    new Thread(() -> {
      try {
        ZkConfigPopulator.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "addConfigToZK").start();

    // in JVM 2
    new Thread(() -> {
      try {
        ZkComponentStarter.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "startComponents").start();

    Thread.sleep(5000);
    // optionally
    new Thread(() -> {
      try {
        ZkConnector.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "startComponents").start();

    Thread.sleep(5000);
    
    // in JVM 3
    new Thread(() -> {
      try {
        ZkComponentStopper.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "startComponents").start();

    log.info("Done");
  }

}
