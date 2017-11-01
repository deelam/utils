package net.deelam.coordworkers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ComponentI;

@Slf4j
public class AmqServiceComp implements ComponentI {

  @Getter
  private boolean running = true;

  @Getter
  private String componentId;

  AmqServiceCompConfig config;

  class AmqServiceCompConfig extends AbstractCompConfig{

    final String brokerUrl;

    // populate and print remaining unused properties
    public AmqServiceCompConfig(Properties props) {
      super(props);
      brokerUrl=useRequiredProperty(props, "brokerUrl");
      checkRemainingProps(props);
    }
  }

  @Override
  public Map<String, Object> getSharedValuesMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("connectionUrl", config.brokerUrl);
    log.info("Sharing values to zookeeper: {}", map);
    return map;
  }
  
  @Override
  public void start(Properties configMap) {
    componentId = configMap.getProperty(COMPONENT_ID);
    log.info("Starting component '{}' with: {}", componentId, configMap);
    config = new AmqServiceCompConfig(configMap);
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
