package net.deelam.zkbasedinit;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AmqComponent extends FancyComponent {

  @Override
  public Map<String, Object> getSharedValuesMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("connectionUrl", "tcp://localhost:61616");
    log.info("Sharing values to zookeeper: {}", map);
    return map;
  }

}
