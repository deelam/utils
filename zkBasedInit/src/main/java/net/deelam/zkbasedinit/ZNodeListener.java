package net.deelam.zkbasedinit;

import org.apache.zookeeper.WatchedEvent;

public interface ZNodeListener{

  default void nodeCreated(String path) {}

  default void nodeDataChanged(String path) {}

  default void nodeDeleted(String path) {}

  default void otherEvent(WatchedEvent evt) {}
  
}