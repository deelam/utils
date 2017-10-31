package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

public class MQClient {

  public static Connection connect(String brokerURL) throws JMSException {
    ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
    Connection connection = factory.createConnection();
    return connection;
  }

  public static Session createRpcSession(String brokerURL) throws JMSException {
    Connection connection = MQClient.connect(brokerURL);
    connection.start();
    return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }
}
