package net.deelam.zkbasedinit;

import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ComponentI;

@Slf4j
public class FancyComponent implements ComponentI {

  @Getter
  private boolean running = true;
  
  @Getter
  private String componentId;

  @Override
  public void start(Properties configMap) {
    componentId=(String) configMap.get(COMPONENT_ID);
    log.info("Starting component '{}' with: {}", componentId, configMap);
    //TODO: populate FancyComponentConfiguration instance and print remaining unused properties
  }

  @Override
  public boolean reinit(Properties configMap) {
    log.info("Reinitializing component '{}' with: {}", componentId, configMap);
    return true;
  }

  @Override
  public void stop() {
    log.info("Stopping component: {}", componentId);
    running = false;
  }

}
