package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerReply implements MessageListener {
  static Logger logger = LoggerFactory.getLogger(ConsumerReply.class);
  static final String QUEUE_NAME = "test";

  private final Connection connection;

  public ConsumerReply(Connection connection) {
    super();
    this.connection = connection;
  }

  private Session session;
  private MessageConsumer consumer;
  private MessageProducer producer;
  Destination queue;

  public static void main(String[] args) throws Exception {
    String brokerURL = Producer.brokerURL;
    Connection connection = MQClient.connect(brokerURL);
    ConsumerReply app = new ConsumerReply(connection);
    logger.info("Starting {}", app);
    app.listen(QUEUE_NAME);
  }

  public void listen(String queueName) {
    try {
      connection.start();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queue = session.createQueue(queueName);
      consumer = session.createConsumer(queue);
      consumer.setMessageListener(this);

      producer = session.createProducer(null);
      //Setup a message producer to respond to messages from clients, we will get the destination
      //to send to from the JMSReplyTo header field from a Message
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

    } catch (Exception e) {
      System.out.println("Caught:" + e);
      e.printStackTrace();
    }

    new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(20000);
            System.out.println(myId + "  msgCount=" + msgCount);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }).start();
  }

  int msgCount = 0;

  private String myId = "id_" + System.currentTimeMillis() % 10;

  public void onMessage(Message message) {
    try {
      if (message instanceof TextMessage) {
        TextMessage txtMessage = (TextMessage) message;
        logger.info("Consumer received: " + txtMessage.getText() + " originally "
            + txtMessage.getStringProperty("consumerId") + " " + message.getJMSReplyTo());
        String consumerId = txtMessage.getStringProperty("consumerId");
        int body = txtMessage.getIntProperty("count"); //Integer.valueOf(txtMessage.getText());

        if (true) {
          --body;
          if (false && body < 100) {
            System.out.println("Exiting early");
            System.exit(0);
          } else if (body > 0) {
            Message response = session.createTextMessage(Integer.toString(body) + " " + myId);
            response.setIntProperty("count", body);
            response.setStringProperty("consumerId", myId);
            response.setJMSCorrelationID(message.getJMSCorrelationID());
            response.setJMSReplyTo(queue);
            producer.send(message.getJMSReplyTo(), response);
          } else {
            System.out.println("Done.");
          }
        } else {
          System.err.println("Ignoring msg originating from other consumers");
        }
      } else {
        System.out.println("Invalid message received.");
      }
      ++msgCount;
    } catch (Exception e) {
      System.err.println("Caught:" + e);
      e.printStackTrace();
    }
  }
}
