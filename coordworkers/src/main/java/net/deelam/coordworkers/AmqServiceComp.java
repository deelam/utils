package net.deelam.coordworkers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.management.RuntimeErrorException;
import org.apache.activemq.broker.BrokerService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.Constants;
import net.deelam.activemq.MQService;
import net.deelam.zkbasedinit.ComponentI;

@Slf4j
public class AmqServiceComp implements ComponentI {

  @Getter
  private boolean running = false;

  public String getComponentId() {
    return config.componentId;
  }

  AmqServiceCompConfig config;
  private BrokerService broker;

  class AmqServiceCompConfig extends AbstractCompConfig {

    final String brokerName;
    final String brokerUrls;

    // populate and print remaining unused properties
    public AmqServiceCompConfig(Properties props) {
      super(props);
      brokerName = useProperty(props, "brokerName", "myAmqBroker");
      brokerUrls = useRequiredProperty(props, "brokerUrl");
      checkRemainingProps(props);
    }
  }

  @Override
  public Map<String, Object> getSharedValuesMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("connectionUrl", config.brokerUrls);
    log.info("Sharing values to zookeeper: {}", map);
    return map;
  }

  @Override
  public void start(Properties configMap) {
    config = new AmqServiceCompConfig(configMap);
    String[] brokerUrls = Constants.parseBrokerUrls(config.brokerUrls);
    if (MQService.jmsServiceExists(brokerUrls[0])) {
      log.error("JMS service already exists at " + brokerUrls[0]);
      throw new IllegalStateException("JMS service already exists at " + brokerUrls[0]);
    } else {
      try {
        broker = MQService.createBrokerService(config.brokerName, brokerUrls);
        running = true;
      } catch (Exception e) {
        log.error("When starting ActiveMQ service", e);
        throw new IllegalStateException("When starting ActiveMQ service", e);
      }
    }
  }

  @Override
  public void stop() {
    log.info("Stopping component: {}", config.componentId);
    if (broker != null) {
      new Thread(() -> {
        while (running)
          try {
            log.info("Delay stopping ActiveMQ service to allow clients to disconnect first ...");
            Thread.sleep(5000);
            if (broker.isStopping()) {
              log.info("Waiting for ActiveMQ service to stop ...");
              Thread.sleep(5000);
            } else {
              log.info("Stopping ActiveMQ service");
              broker.stop();
            }
          } catch (Exception e) {
            log.error("When stopping ActiveMQ service", e);
          } finally {
            running = !broker.isStopped();
          }
      }, "delayedAmqShutdown").start();
    }
  }


}
