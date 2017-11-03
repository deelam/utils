package net.deelam.coordworkers;

import java.util.Properties;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.Constants;
import net.deelam.activemq.MQClient;
import net.deelam.zkbasedinit.ComponentI;

@Slf4j
public class JobberComp implements ComponentI {

  @Getter
  private boolean running = true;

  public String getComponentId() {
    return config.componentId;
  }

  SubmitterCompConfig config;

  class SubmitterCompConfig extends AbstractCompConfig {

    final String brokerUrl;
    final String submitJobQueue;
    final String jobDoneTopic;
    final String jobFailedTopic;
    final String getJobsTopic;
    final String availJobsTopic;
    final String pickedJobQueue;
    int deliveryMode = DeliveryMode.NON_PERSISTENT;


    // populate and print remaining unused properties
    public SubmitterCompConfig(Properties props) {
      super(props);
      brokerUrl = Constants.getTcpBrokerUrl(useRequiredRefProperty(props, "brokerUrl.ref"));
      submitJobQueue = useRequiredProperty(props, "msgQ.submitJob");
      jobDoneTopic = useRequiredProperty(props, "msgT.jobDone");
      jobFailedTopic = useRequiredProperty(props, "msgT.jobFailed");
      getJobsTopic = useRequiredProperty(props, "msgT.getJobs");
      availJobsTopic = useRequiredProperty(props, "msgT.availJobs");
      pickedJobQueue = useProperty(props, "msgQ.pickedJob", availJobsTopic + ".pickedJob");
      checkRemainingProps(props);
    }

  }

  private Connection conn;
  private Session session;

  @Override
  public void start(Properties configMap) {
    config = new SubmitterCompConfig(configMap);
    try {
      conn = MQClient.connect(config.brokerUrl);
      conn.start();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue pickedJobQueue = session.createQueue(config.pickedJobQueue);
      MessageProducer msgResponder =
          MQClient.createGenericMsgResponder(session, config.deliveryMode);

      handleSubmitJobMsg(msgResponder, session.createTopic(config.availJobsTopic), pickedJobQueue);

      MsgHelper.listenToJobStatusMsgs(session, null, config.jobDoneTopic, config.jobFailedTopic);

      handlePickedJobMsg(pickedJobQueue, msgResponder);
      handleGetJobsMsg(msgResponder, pickedJobQueue);

      running = true;
    } catch (JMSException e) {
      log.error("When connecting to JMS service", e);
    }
  }

  @Override
  public void stop() {
    log.info("Stopping component: {}", config.componentId);
    try {
      conn.close();
    } catch (JMSException e) {
      log.error("When closing JMS session", e);
    }
    running = false;
  }

  private void handleSubmitJobMsg(MessageProducer msgResponder, Destination availJobsTopic,
      Queue pickedJobQueue) {
    MQClient.createQueueConsumer(session, config.submitJobQueue, message -> {
      if (message instanceof TextMessage) {
        TextMessage txtMessage = (TextMessage) message;
        String currMsg = txtMessage.getText();
        log.info("Message received: " + currMsg);

        sendJobsAvailableMsg(msgResponder, null, availJobsTopic, pickedJobQueue);
      } else {
        log.error("Invalid message received: {}", message);
      }
    });
  }

  private void handleGetJobsMsg(MessageProducer msgResponder, Destination pickedJobQueue) {
    MQClient.createTopicConsumer(session, config.getJobsTopic, message -> {
      if (message instanceof TextMessage) {
        TextMessage txtMessage = (TextMessage) message;
        String currMsg = txtMessage.getText();
        log.info("Message received: " + currMsg);

        sendJobsAvailableMsg(msgResponder, message.getJMSCorrelationID(), message.getJMSReplyTo(),
            pickedJobQueue);
      } else {
        log.error("Invalid message received: {}", message);
      }
    });
  }

  private void sendJobsAvailableMsg(MessageProducer msgResponder, String correlationID,
      Destination dest, Destination pickedJobQueue) {
    try {
      msgResponder.send(dest, createJobsAvailableMsg(correlationID, pickedJobQueue));
    } catch (JMSException e) {
      log.error("When sending to availJobsTopic", e);
    }
  }

  private Message createJobsAvailableMsg(String correlationID, Destination pickedJobQueue)
      throws JMSException {
    int body = 1; // txtMessage.getIntProperty("count"); //Integer.valueOf(txtMessage.getText());
    Message response = session.createTextMessage(Integer.toString(body) + " " + getComponentId());
    response.setIntProperty("count", body);
    response.setStringProperty("senderComponentId", getComponentId());
    response.setJMSCorrelationID(correlationID);
    response.setJMSReplyTo(pickedJobQueue);
    return response;
  }

  private void handlePickedJobMsg(Queue pickedJobQueue, MessageProducer msgResponder)
      throws JMSException {
    MessageConsumer consumer = session.createConsumer(pickedJobQueue);
    consumer.setMessageListener(m -> {
      log.info("Got pickedJobMsg: {}", m);
      try {
        Message response;
        if(Math.random()*2>1)
          response=createConfirmPickedJobResponse(m);
        else
          response=createRejectPickedJobResponse(m);
        msgResponder.send(m.getJMSReplyTo(), response);
      } catch (JMSException e) {
        log.error("When processing message: {}", m, e);
      }
    });
  }

  private Message createConfirmPickedJobResponse(Message rcvMsg) throws JMSException {
    Message response = session.createTextMessage(getComponentId() + " confirm picked job ");
    response.setStringProperty("senderComponentId", getComponentId());
    response.setBooleanProperty("goAhead", true);
    return response;
  }
  
  private Message createRejectPickedJobResponse(Message rcvMsg) throws JMSException {
    Message response = session.createTextMessage(getComponentId() + " reject picked job ");
    response.setStringProperty("senderComponentId", getComponentId());
    response.setBooleanProperty("goAhead", false);
    return response;
  }
}
