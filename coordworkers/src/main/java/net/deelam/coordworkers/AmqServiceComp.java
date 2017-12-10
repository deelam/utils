package net.deelam.coordworkers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.ConstantsAmq;
import net.deelam.activemq.MQService;
import net.deelam.utils.ConsoleLogging;
import net.deelam.zkbasedinit.ComponentI;

@Slf4j
public class AmqServiceComp implements ComponentI {
  static final Logger clog=ConsoleLogging.createSlf4jLogger(AmqServiceComp.class);

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
    final String tcpBrokerUrl;

    // populate and print remaining unused properties
    public AmqServiceCompConfig(Properties props) {
      super(props);
      brokerName = useProperty(props, "brokerName", "myAmqBroker");
      brokerUrls = useRequiredProperty(props, "brokerUrls");
      tcpBrokerUrl = ConstantsAmq.getTcpBrokerUrl(brokerUrls);

      checkRemainingProps(props);
    }
  }

  @Override
  public Map<String, Object> getSharedValuesMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("connectionUrls", config.brokerUrls);
    map.put("connectionTcpUrl", config.tcpBrokerUrl);
    log.info("Sharing values to zookeeper: {}", map);
    return map;
  }

  @Override
  public void start(Properties configMap) {
    config = new AmqServiceCompConfig(configMap);
    String[] brokerUrls = ConstantsAmq.parseBrokerUrls(config.brokerUrls);
    
    log.info("System.setProperty: {}={}", ConstantsAmq.BROKER_URL, config.tcpBrokerUrl);
    System.setProperty(ConstantsAmq.BROKER_URL, config.tcpBrokerUrl);

    if (MQService.jmsServiceExists(config.tcpBrokerUrl)) {
      log.error("JMS service already exists at " + config.tcpBrokerUrl);
      throw new IllegalStateException("JMS service already exists at " + config.tcpBrokerUrl);
    } else {
      try {
        broker = MQService.createBrokerService(config.brokerName, brokerUrls);
        running = true;
      } catch (Exception e) {
        log.error("When starting ActiveMQ service", e);
        throw new IllegalStateException("When starting ActiveMQ service", e);
      }
    }
    log.info("COMP: Started {}", config.getComponentId());
  }

  @Override
  public void stop() {
    log.info("Stopping component: {}", config.componentId);
    if (broker != null) {
      new Thread(() -> {
        while (running)
          try {
            clog.info("-- Delaying ActiveMQ shutdown to allow clients to disconnect...");
            Thread.sleep(5000);
            if (broker.isStopping()) {
              clog.info("-- Waiting for ActiveMQ service to stop ... currConnections="+broker.getCurrentConnections());
              Thread.sleep(2000);
            } else {
              clog.info("-- Stopping ActiveMQ service");
              broker.stop();
              //Thread.sleep(2000);
            }
          } catch (Exception e) {
            log.error("-- When stopping ActiveMQ service", e);
          } finally {
            running = !broker.isStopped();
          }
      }, "delayedAmqShutdown").start();
    }
  }


}
