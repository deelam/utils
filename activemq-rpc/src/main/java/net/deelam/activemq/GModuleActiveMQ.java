package net.deelam.activemq;

import java.util.Arrays;
import javax.inject.Named;
import org.apache.activemq.broker.BrokerService;
import com.esotericsoftware.minlog.Log;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GModuleActiveMQ extends AbstractModule {

  private final String amqBrokerName;
  private final String[] amqBrokerURLs;

  public GModuleActiveMQ(String amqBrokerName, String... amqBrokerURLs) {
    super();
    this.amqBrokerName = amqBrokerName;
    this.amqBrokerURLs = amqBrokerURLs;
  }

  @Override
  public void configure() {
    {
      String brokerName = System.getProperty(Constants.BROKER_NAME);
      if (brokerName == null || brokerName.length()==0)
        brokerName = amqBrokerName;
      if (brokerName == null)
        brokerName = "amqBroker-" + System.currentTimeMillis();
      log.info("System.setProperty: {}={}", Constants.BROKER_NAME, brokerName);
      System.setProperty(Constants.BROKER_NAME, brokerName);
      bind(String.class).annotatedWith(Names.named(Constants.BROKER_NAME)).toInstance(brokerName);
    }
    {
      String[] brokerUrls = null;
      String brokerUrlStr = System.getProperty(Constants.BROKER_URL);
      if (brokerUrlStr == null || brokerUrlStr.length()==0) {
        brokerUrls = amqBrokerURLs;
        if (brokerUrls == null)
          brokerUrls = new String[] {"tcp://localhost:61616", "stomp://localhost:61613"};
      }
      if (brokerUrls == null)
        brokerUrls =
            Arrays.stream(brokerUrlStr.split(",")).map(String::trim).toArray(String[]::new);

      String tcpBrokerUrl = Constants.getTcpBrokerUrl(brokerUrlStr);
      log.info("System.setProperty: {}={}", Constants.BROKER_URL, tcpBrokerUrl);
      System.setProperty(Constants.BROKER_URL, tcpBrokerUrl);
      bind(String.class).annotatedWith(Names.named(Constants.BROKER_URL)).toInstance(tcpBrokerUrl);
      bind(String[].class).annotatedWith(Names.named(Constants.BROKER_URLS)).toInstance(brokerUrls);
    }
  }

  @Provides
  protected BrokerService createBrokerService(@Named(Constants.BROKER_URL) String brokerURL,
      @Named(Constants.BROKER_URLS) String[] brokerURLs,
      @Named(Constants.BROKER_NAME) String brokerName) {
    if (!MQService.jmsServiceExists(brokerURL)) {
      try {
        return MQService.createBrokerService(brokerName, brokerURLs);
      } catch (Exception e) {
        Log.error("When creating ActiveMQ Broker service", e);
      }
    }
    return null;
  }

}
