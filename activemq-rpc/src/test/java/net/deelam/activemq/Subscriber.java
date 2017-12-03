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

public class Subscriber implements MessageListener {
  static final String TOPIC_NAME = "test2";

  private final Connection connection;
  public Subscriber(Connection connection) {
    super();
    this.connection = connection;
  }

  private Session session;
  private MessageConsumer consumer;
  private MessageProducer producer;
  
  public static void main(String[] args) throws Exception {
    String brokerURL = Producer.brokerURL;
    Connection connection = MQClient.connect(brokerURL);
    Subscriber app = new Subscriber(connection);
    app.listen(TOPIC_NAME);
  }

  public void listen(String queueName) {
    try {
      connection.start();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination queue = session.createTopic(queueName);
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

  private String myId = "id_" + System.currentTimeMillis() % 10;
  int msgCount=0;
  public void onMessage(Message message) {
    try {
      if (message instanceof TextMessage) {
        TextMessage txtMessage = (TextMessage) message;
        txtMessage.getText();
        //System.out.println("Message received: " + txtMessage.getText());
        {
          int body = txtMessage.getIntProperty("count");
          Message response = session.createTextMessage(Integer.toString(body) + " " + myId);
          response.setIntProperty("count", body);
          response.setStringProperty("consumerId", myId);
          response.setJMSCorrelationID(message.getJMSCorrelationID());
          //response.setJMSReplyTo(queue);
          producer.send(message.getJMSReplyTo(), response);
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
