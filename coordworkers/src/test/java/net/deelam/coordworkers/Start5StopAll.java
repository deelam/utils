package net.deelam.coordworkers;

import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ZkComponentStopper;

@Slf4j
public final class Start5StopAll {
  public static void main(String[] args) {
    try {
      ZkComponentStopper.main(args);
      Thread.sleep(1000);
    } catch (Exception e) {
      log.info("Tree doesn't exist yet");
    }
  }
}
