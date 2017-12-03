package net.deelam.activemq;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Publisher {
  private Connection connection;
  private Session session;
  TopicUtils topicUtils;

  public Publisher(Connection connection, String topicName) throws JMSException {
    this.connection = connection;
    connection.start();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination dest = session.createTopic(topicName);
    topicUtils = new TopicUtils(session, dest);
    topicUtils.listenToTopicConsumerResponses("responseConsumerId", null);
    //topicUtils.listenToTopicConsumerResponses("responseConsumerId", TopicUtils.getTextMessageMapper());
  }

  public void run() throws JMSException, InterruptedException, ExecutionException {
    for (int i = 0; i < 5; i++) {
      // System.out.println("Creating Message " + i);
      Message message = session.createTextMessage("Hello World! " + i);
      message.setIntProperty("count", 1000);
      List<String> answer = topicUtils.query("responseConsumerId", message);
      log.info("answer={}", answer);
      Thread.sleep(3_000);
    }
  }

  public void close() throws JMSException, IOException {
    topicUtils.close();
    if (connection != null) {
      connection.close();
    }
  }

  public static String brokerURL = "tcp://localhost:61616";

  public static void main(String[] args) throws Exception {
    // setup the connection to ActiveMQ
    Connection connection = MQClient.connect(brokerURL);

    Publisher producer = new Publisher(connection, Subscriber.TOPIC_NAME);
    producer.run();
    Thread.sleep(12_000);
    producer.close();
  }
}
