package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer implements MessageListener {
  static Logger logger = LoggerFactory.getLogger(Consumer.class);
  static final String QUEUE_NAME = "test";

  private final Connection connection;
  public Consumer(Connection connection) {
    super();
    this.connection = connection;
  }

  private Session session;
  private MessageConsumer consumer;

  public static void main(String[] args) throws Exception {
    String brokerURL = Producer.brokerURL;
    Connection connection = MQClient.connect(brokerURL);
    Consumer app = new Consumer(connection);
    app.listen(QUEUE_NAME);
  }

  private String currMsg;
  public void listen(String queueName) {
    try {
      connection.start();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination queue = session.createQueue(queueName);
      consumer = session.createConsumer(queue);
      consumer.setMessageListener(this);
    } catch (Exception e) {
      System.out.println("Caught:" + e);
      e.printStackTrace();
    }
    
    new Thread(new Runnable() {
      @Override
      public void run() {
        while(true){
          try {
            Thread.sleep(1000);
            System.out.println("msgCount="+msgCount+" "+currMsg);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }).start();
  }

  int msgCount=0;
  public void onMessage(Message message) {
    try {
      if (message instanceof TextMessage) {
        TextMessage txtMessage = (TextMessage) message;
        currMsg=txtMessage.getText();
        //System.out.println("Message received: " + txtMessage.getText());
        try {
          //logger.info("Sleeping");
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        //System.out.println("Invalid message received.");
      }
      ++msgCount;
    } catch (JMSException e) {
      System.out.println("Caught:" + e);
      e.printStackTrace();
    }
  }
}
