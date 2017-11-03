package net.deelam.coordworkers;

import net.deelam.zkbasedinit.ZkComponentStarter;

public final class Start2Amq {
  public static void main(String[] args) {
    try {
      System.setProperty("componentIds", "amq");
      ZkComponentStarter.main(args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
