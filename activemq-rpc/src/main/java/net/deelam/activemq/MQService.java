package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQService {
  static Logger logger = LoggerFactory.getLogger(MQService.class);

  public static void main(String[] args) throws Exception {
    String brokerURL = "tcp://localhost:61616";
    if (!jmsServiceExists(brokerURL)) {
      createBrokerService("aBrokerName", brokerURL, "stomp://localhost:61613");
    }
  }

  public static boolean jmsServiceExists(String brokerURL) {
    ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
    try {
      Connection connection = factory.createConnection();
      connection.close();
      return true;
    } catch (JMSException e) {
      return false;
    }
  }

  public static BrokerService createBrokerService(String brokerName, String... bindAddresses) throws Exception {
    BrokerService broker = new BrokerService();
    logger.info("Created broker '{}'", brokerName);
    broker.setBrokerName(brokerName);
    // configure the broker
    for (String addr : bindAddresses){
      logger.info("Binding to address: {}",addr);
      broker.addConnector(addr);
    }
    broker.start();
    return broker;
  }

}
