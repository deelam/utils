package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import net.deelam.activemq.MQClient;

public class ProducerHeaders {
  private Connection connection;
  private Session session;
  private MessageProducer producer;

  public ProducerHeaders(Connection connection, String queueName) throws JMSException {
    this.connection=connection;
    connection.start();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createQueue(queueName);
    producer = session.createProducer(destination);
    //use if need speed: 
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
  }

  public void run() throws JMSException {
    for (int i = 0; i < 1; i++) {
      System.out.println("Creating Message " + i);
      Message message = session.createTextMessage("Hello World! " + i);
      message.setStringProperty("domainfieldsFile", "/home/dlam/dev/mysql-ingest/domainfields.intel.conf"); 
      message.setStringProperty("fieldmapFile", "/home/dlam/dev/mysql-ingest/fieldmap.TIDE.conf");
      message.setStringProperty("dbname", "thegeekstuff");
      message.setStringProperty("tablename", "table"+System.currentTimeMillis());
      message.setStringProperty("csvFile", "/tmp/data/INTEL_datasets/TIDE_sample_data.csv");
      //message.setStringProperty("methodName", "meth1");
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

    ProducerHeaders producer = new ProducerHeaders(connection, Consumer.QUEUE_NAME);
    producer.run();
    producer.close();
  }
}
