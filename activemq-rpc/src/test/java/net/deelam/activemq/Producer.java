package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import net.deelam.activemq.MQClient;

public class Producer {
  private Connection connection;
  private Session session;
  private MessageProducer producer;

  public Producer(Connection connection, String queueName) throws JMSException {
    this.connection=connection;
    connection.start();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createQueue(queueName);
    producer = session.createProducer(destination);
    //use if need speed: 
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
  }

  public void run() throws JMSException {
    for (int i = 0; i < 1000; i++) {
      System.out.println("Creating Message " + i);
      Message message = session.createTextMessage("Hello World! " + i);
      message.setStringProperty("param1", "value1");
      message.setStringProperty("param2", "/some/long/path/to/a/file.csv");
      producer.send(message);
    }
  }

  public void close() throws JMSException {
    if (connection != null) {
      connection.close();
    }
  }

  public static String brokerURL = "tcp://localhost:61616";  // should match MQService.brokerURL

  public static void main(String[] args) throws Exception {
    // setup the connection to ActiveMQ
    Connection connection = MQClient.connect(brokerURL);

    Producer producer = new Producer(connection, Consumer.QUEUE_NAME);
    producer.run();
    producer.close();
  }
}
