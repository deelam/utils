package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ConsumerTopic implements MessageListener {
  static final String QUEUE_NAME = "test";

  private final Connection connection;
  public ConsumerTopic(Connection connection) {
    super();
    this.connection = connection;
  }

  private Session session;
  private MessageConsumer consumer;

  public static void main(String[] args) throws Exception {
    String brokerURL = Producer.brokerURL;
    Connection connection = MQClient.connect(brokerURL);
    ConsumerTopic app = new ConsumerTopic(connection);
    app.listen(QUEUE_NAME);
  }

  public void listen(String queueName) {
    try {
      connection.start();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination queue = session.createTopic(queueName);
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
            Thread.sleep(2000);
            System.out.println("msgCount="+msgCount);
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
        txtMessage.getText();
        //System.out.println("Message received: " + txtMessage.getText());
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
