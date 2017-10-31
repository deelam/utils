package net.deelam.zkbasedinit;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public interface ComponentI {

  /// keys for configMap
  String ZK_PATH = "_zookeeperPath";
  String COMPONENT_ID = "_componentId";
  
  String getComponentId();

  void start(Properties configMap);
  
  boolean reinit(Properties configMap);

  boolean isRunning();

  /** 
   * After start() or reinit() are called, the entries returned by this method 
   * are used to set values for the corresponding path (key) in Zookeeper
   * @return
   */
  default Map<String, Object> getSharedValuesMap() { return Collections.emptyMap();}

  void stop();

}
