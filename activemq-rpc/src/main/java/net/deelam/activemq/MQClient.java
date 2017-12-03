package net.deelam.activemq;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MQClient {

  public static Connection connect(String brokerURL) throws JMSException {
    Preconditions.checkNotNull(brokerURL);
    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
    factory.setTrustAllPackages(true); // Needed for sending ObjectMessages http://activemq.apache.org/objectmessage.html
    Connection connection = factory.createConnection();
    return connection;
  }

  public static Session createRpcSession(Connection connection) throws JMSException {
    return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  public static interface MsgHandler {
    void handle(Message msg) throws JMSException;
  }

  public static MessageConsumer createQueueConsumer(Session session, String queueName,
      MsgHandler listener) {
    try {
      return createConsumerFor(session.createQueue(queueName), session, listener);
    } catch (JMSException e) {
      log.error("When creating Queue for: {}", queueName, e);
      throw new RuntimeException(e);
    }
  }

  public static MessageConsumer createTopicConsumer(Session session, String topicName,
      MsgHandler listener) {
    try {
      return createConsumerFor(session.createTopic(topicName), session, listener);
    } catch (JMSException e) {
      log.error("When creating Topic for: {}", topicName, e);
      throw new RuntimeException(e);
    }
  }

  public static MessageConsumer createConsumerFor(Destination dest, Session session,
      MsgHandler listener) {
    try {
      MessageConsumer consumer = session.createConsumer(dest);
      consumer.setMessageListener(msg -> {
        try {
          listener.handle(msg);
        } catch (JMSException e) {
          log.error("When reading message: {}", msg, e);
        }
      });
      return consumer;
    } catch (JMSException e) {
      log.error("When creating MessageConsumer for: {}", dest, e);
      throw new RuntimeException(e);
    }
  }

  public static interface MsgSender {
    void send(Message msg) throws JMSException;
  }

  public static MessageProducer createQueueMsgSender(Session session, String queueName,
      int deliveryMode) {
    try {
      MessageProducer producer = session.createProducer(session.createQueue(queueName));
      // use NON_PERSISTENT if need speed
      producer.setDeliveryMode(deliveryMode);
      return producer;
    } catch (JMSException e) {
      log.error("When creating MessageProducer for: {}", queueName, e);
      throw new RuntimeException(e);
    }
  }

  public static MessageProducer createTopicMsgSender(Session session, String topicName,
      int deliveryMode) {
    try {
      MessageProducer producer = session.createProducer(session.createTopic(topicName));
      // use NON_PERSISTENT if need speed
      producer.setDeliveryMode(deliveryMode);
      return producer;
    } catch (JMSException e) {
      log.error("When creating MessageProducer for: {}", topicName, e);
      throw new RuntimeException(e);
    }
  }

  public static MessageProducer createGenericMsgResponder(Session session, int deliveryMode)
      throws JMSException {
    MessageProducer prod = session.createProducer(null);
    // Setup a message producer to respond to messages from clients.
    // Get the destination from the JMSReplyTo message header field.
    prod.setDeliveryMode(deliveryMode);
    return prod;
  }
}
