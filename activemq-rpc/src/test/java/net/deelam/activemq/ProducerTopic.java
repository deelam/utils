package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import net.deelam.activemq.MQClient;

public class ProducerTopic {
  private Connection connection;
  private Session session;
  private MessageProducer producer;

  public ProducerTopic(Connection connection, String topicName) throws JMSException {
    this.connection = connection;
    connection.start();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination dest = session.createTopic(topicName);
    producer = session.createProducer(dest);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
  }

  public void run() throws JMSException {
    for (int i = 0; i < 10000; i++) {
      //System.out.println("Creating Message " + i);
      Message message = session.createTextMessage("Hello World! " + i);
      producer.send(message);
    }
  }

  public void close() throws JMSException {
    if (connection != null) {
      connection.close();
    }
  }

  public static String brokerURL = "tcp://localhost:61616";

  public static void main(String[] args) throws Exception {
    // setup the connection to ActiveMQ
    Connection connection = MQClient.connect(brokerURL);

    ProducerTopic producer = new ProducerTopic(connection, Consumer.QUEUE_NAME);
    producer.run();
    producer.close();
  }
}
