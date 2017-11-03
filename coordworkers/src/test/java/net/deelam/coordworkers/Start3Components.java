package net.deelam.coordworkers;

import net.deelam.zkbasedinit.ZkComponentStarter;

public final class Start3Components {
  public static void main(String[] args) {
    try {
      System.setProperty("componentIds", "jobberB, workerType");
      ZkComponentStarter.main(args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
