package net.deelam.activemq;

import java.io.Closeable;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * When publishing a message, must specify which response consumer (and hence response mapper) to
 * use.
 * 
 * <pre>
 *
 * <pre>
 * 
 * @param <T>
 */
@Slf4j
public class TopicUtils implements Closeable {

  private Session session;
  private ConsumerCounter subscribersCounter;
  private Timer responseTimer;
  private MessageProducer producer;

  @Setter
  private long responseTimeout = 10; // in seconds to wait for all consumers' responses

  public TopicUtils(Session session, Destination topic) throws JMSException {
    this(session, topic, session.createProducer(topic));
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
  }

  public TopicUtils(Session session, Destination topic, MessageProducer producer)
      throws JMSException {
    this.session = session;
    this.producer = producer;
    subscribersCounter = new ConsumerCounter(session, topic);
    responseTimer = new Timer();
  }

  private Map<String, CombinedResponse<?>> correlId2responses = new HashMap<>();

  public <T> List<T> query(String responseConsumerId, Message message) throws JMSException {
    CombinedResponse<T> responseF = queryAsync(responseConsumerId, message);
    List<T> response;
    try {
      response = responseF.getFuture().get(responseTimeout, TimeUnit.SECONDS);
      log.info("response={}", response);
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      response = responseF.getResponses();
      log.info("response so far={}", response, e);
    }
    correlId2responses.remove(responseF.getCorrelationID());
    return response;
  }

  public <T> CombinedResponse<T> queryAsync(String responseConsumerId, Message message)
      throws JMSException {
    Destination replyInbox = replyInboxes.get(responseConsumerId);
    if (replyInbox == null) {
      throw new IllegalStateException(
          "Must initialize by calling listenToTopicConsumerResponses() for responseConsumerName="
              + responseConsumerId);
    }

    CombinedResponse<T> responseF = new CombinedResponse<>(subscribersCounter.getCount());
    correlId2responses.put(responseF.getCorrelationID(), responseF);
    // ensure correlId2responses entries are removed after timeout
    responseTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        if (correlId2responses.remove(responseF.getCorrelationID()) != null)
          if(!responseF.getFuture().isDone())
            log.info("Removed timed-out CombinedResponse: {}", responseF);
          else
            log.debug("Removed completed CombinedResponse: {}", responseF);
      }
    }, (responseTimeout + 1) * 1000);

    message.setJMSCorrelationID(responseF.getCorrelationID());
    message.setJMSReplyTo(replyInbox);
    producer.send(message);
    return responseF;
  }

  private Map<String, TemporaryQueue> replyInboxes = new HashMap<>();
  private Map<String, MessageConsumer> replyConsumers = new HashMap<>();

  public <T> MessageConsumer listenToTopicConsumerResponses(String responseConsumerId,
      Function<Message, T> responseMapper) throws JMSException {
    TemporaryQueue replyInbox = session.createTemporaryQueue();
    MessageConsumer responseConsumer = session.createConsumer(replyInbox);
    replyInboxes.put(responseConsumerId, replyInbox);
    replyConsumers.put(responseConsumerId, responseConsumer);
    responseConsumer.setMessageListener((message) -> {
      try {
        log.info("Producer received: {}", message);
        String correlId = message.getJMSCorrelationID();
        CombinedResponse<T> combinedResponse =
            (CombinedResponse<T>) correlId2responses.get(correlId);
        if (combinedResponse == null)
          log.error("Unknown correlId={}: {}", correlId, message);
        else if (responseMapper == null)
          combinedResponse.addResponse((T) getDefaultMessageMapper().apply(message));
        else
          combinedResponse.addResponse(responseMapper.apply(message));
      } catch (Exception e) {
        log.warn("When reading consumer response:" + e);
      }
    });
    return responseConsumer;
  }

  public static Function<Message, String> getTextMessageMapper() {
    return message -> {
      try {
        if (message instanceof TextMessage) {
          TextMessage txtMessage = (TextMessage) message;
          log.info("Received: " + txtMessage.getText());
          return txtMessage.getText();
        } else {
          log.error("Expecting TextMessage but got {}", message.getClass());
        }
      } catch (JMSException e) {
        log.error("Expect");
      }
      return null;
    };
  }

  public static Function<Message, Object> getDefaultMessageMapper() {
    return message -> {
      try {
        log.info("Received: " + message);
        if (message instanceof TextMessage) {
          return ((TextMessage) message).getText();
        } else if (message instanceof MapMessage) {
          MapMessage mMess = (MapMessage) message;
          Map<Object, Object> map = new HashMap<>();
          for (Enumeration<String> e = mMess.getMapNames(); e.hasMoreElements();) {
            String key = e.nextElement();
            map.put(key, mMess.getObject(key));
          }
          return map;
        } else if (message instanceof ObjectMessage) {
          // Note: http://activemq.apache.org/objectmessage.html
          return ((ObjectMessage) message).getObject();
        } else if (message instanceof BytesMessage) {
          BytesMessage bMess = (BytesMessage) message;
          byte[] body = new byte[(int) bMess.getBodyLength()];
          bMess.readBytes(body);
          return body;
        } else {
          log.error("Cannot handle message type: {}", message);
        }
      } catch (JMSException e) {
        log.error("When handling message", e);
      }
      return null;
    };
  }

  @Override
  public void close() throws IOException {
    replyConsumers.values().forEach(cons -> {
      try {
        cons.close();
      } catch (JMSException e) {
        log.warn("When closing: {}", cons, e);
      }
    });
    // must consumer.close() before inbox.delete()
    replyInboxes.values().forEach(inbox -> {
      try {
        inbox.delete();
      } catch (JMSException e) {
        log.warn("When deleting: {}", inbox, e);
      }
    });
    log.info("correlId2responses size={}", correlId2responses.size());
    responseTimer.cancel();
  }

}
