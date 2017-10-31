package net.deelam.zkbasedinit;

import java.util.Properties;

public interface ComponentI {

  /// keys for configMap
  String ZK_PATH = "_zookeeperPath";
  String COMPONENT_ID = "_componentId";
  
  String getComponentId();

  void start(Properties configMap);

  boolean reinit(Properties configMap);

  boolean isRunning();

  void stop();

}
