package net.deelam.zkbasedinit;

import java.util.function.Consumer;

public interface ZkComponentStarterI {

  void setComponentStartedCallback(Consumer<ComponentI> startedCb);

  void setStarterCompleteCallback(Consumer<ComponentI> completedCb);

  String getComponentId();

}
