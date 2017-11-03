package net.deelam.coordworkers;

import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ZkComponentStopper;
import net.deelam.zkbasedinit.ZkConfigPopulator;

@Slf4j
public final class Start1ConfigPopulator {
  public static void main(String[] args) {
    try {
      ZkComponentStopper.main(args);
      Thread.sleep(1000);
    } catch (Exception e) {
      log.info("Tree doesn't exist yet");
    }

    // in JVM 1, Start ZK and populate
    try {
      System.setProperty("componentIds", "amq, submitterA, jobberB, workerType");
      ZkConfigPopulator.main(new String[] {"configs.props"});
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
