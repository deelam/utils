package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.deelam.activemq.MQClient;

public class ProducerReply {
  static Logger logger = LoggerFactory.getLogger(ProducerReply.class);
  
  private Connection connection;
  private Session session;
  private MessageProducer producer;
  Destination tempDest;

  public ProducerReply(Connection connection, String queueName) throws JMSException {
    this.connection=connection;
    connection.start();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createQueue(queueName);
    producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    
    tempDest = session.createTemporaryQueue();
    MessageConsumer responseConsumer = session.createConsumer(tempDest);
    responseConsumer.setMessageListener(new MessageListener() {
      @Override
      public void onMessage(Message message) {
        try {
          if (message instanceof TextMessage) {
            TextMessage txtMessage = (TextMessage) message;
            int body=txtMessage.getIntProperty("count"); //Integer.valueOf(txtMessage.getText());
            logger.info("Producer: received: " + txtMessage.getText()+" "+message.getJMSReplyTo());
            
            --body;
            if(body>0){
              Message response = session.createTextMessage(Integer.toString(body));
              response.setIntProperty("count", body);
              response.setStringProperty("consumerId", txtMessage.getStringProperty("consumerId"));
              response.setJMSReplyTo(tempDest);
              response.setJMSCorrelationID(message.getJMSCorrelationID());
              producer.send(message.getJMSReplyTo(), response);        
            } else {
              System.out.println("Done.");
            }
          } else {
            System.out.println("Invalid message received.");
          }
        } catch (Exception e) {
          System.err.println("Caught:" + e);
          e.printStackTrace();
        }
      }
    });
  }

  public void run() throws JMSException {
    for (int i = 0; i < 100; i++) {
      System.out.println("Creating Message " + i);
      Message message = session.createTextMessage(Integer.toString(1000));
      message.setIntProperty("count", 1000);
      message.setJMSReplyTo(tempDest);
      message.setJMSCorrelationID("myCorrelationID");
      producer.send(message);
      break;
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

    ProducerReply producer = new ProducerReply(connection, ConsumerReply.QUEUE_NAME);
    producer.run();
    Thread.sleep(10000);
    producer.close();
  }
}
