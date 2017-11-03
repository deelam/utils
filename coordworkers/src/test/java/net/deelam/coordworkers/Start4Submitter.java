package net.deelam.coordworkers;

import net.deelam.zkbasedinit.ZkComponentStarter;

public final class Start4Submitter {
  public static void main(String[] args) {
    try {
      System.setProperty("componentIds", "submitterA");
      ZkComponentStarter.main(args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
