package net.deelam.zkbasedinit;

import java.util.function.Consumer;

public interface ZkComponentStarterI {
  final String COPIES_SUBPATH = "/copies";
  
  void setComponentStartedCallback(Consumer<ComponentI> startedCb);

  void setStarterCompleteCallback(Consumer<ComponentI> completedCb);

  String getComponentId();

}
