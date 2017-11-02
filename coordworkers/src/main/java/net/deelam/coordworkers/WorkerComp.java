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
public class WorkerComp implements ComponentI {

  @Getter
  private boolean running = true;

  public String getComponentId() {
    return config.componentId;
  }

  SubmitterCompConfig config;

  class SubmitterCompConfig extends AbstractCompConfig {

    final String brokerUrl;
    final String submitJobQueue;
    final String jobStateTopic;
    final String jobDoneTopic;
    final String jobFailedTopic;
    final String getJobsTopic;
    final String availJobsTopic;
    int deliveryMode = DeliveryMode.NON_PERSISTENT;
    final String confirmPickQueue;

    // populate and print remaining unused properties
    public SubmitterCompConfig(Properties props) {
      super(props);
      brokerUrl = Constants.getTcpBrokerUrl(useRequiredRefProperty(props, "brokerUrl.ref"));
      submitJobQueue = useRequiredProperty(props, "msgQ.submitJob");
      jobStateTopic = useRequiredProperty(props, "msgT.jobState");
      jobDoneTopic = useRequiredProperty(props, "msgT.jobDone");
      jobFailedTopic = useRequiredProperty(props, "msgT.jobFailed");
      getJobsTopic = useRequiredProperty(props, "msgT.getJobs");
      availJobsTopic = useRequiredProperty(props, "msgT.availJobs");
      confirmPickQueue = useProperty(props, "msgQ.confirmPick", availJobsTopic + ".confirmPick");
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

      MessageProducer msgResponder =
          MQClient.createGenericMsgResponder(session, config.deliveryMode);

      handleAvailJobsMsg(msgResponder, handleConfirmPickQueueMsg());

      // TODO: periodically send msg to getJobsTopic

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

  boolean acceptingJobs=true;

  private void handleAvailJobsMsg(MessageProducer msgResponder, Destination confirmedQueue) {
    MQClient.createTopicConsumer(session, config.availJobsTopic, message -> {
      if (acceptingJobs) {
        if (message instanceof TextMessage) {
          acceptingJobs = false;
          TextMessage txtMessage = (TextMessage) message;
          String currMsg = txtMessage.getText();
          log.info("Message received: " + currMsg);

          msgResponder.send(message.getJMSReplyTo(), createPickedJobResponse(confirmedQueue));
        } else {
          log.error("Invalid message received: {}", message);
        }
      } else {
        log.info("Busy working; ignoring received message: " + message);
      }
    });
  }

  private Message createPickedJobResponse(Destination confirmedQueue) throws JMSException {
    Message response = session.createTextMessage(getComponentId() + " picked job ");
    response.setStringProperty("senderComponentId", getComponentId());
    String jobID = getComponentId() + "-pick-" + System.currentTimeMillis();
    response.setJMSCorrelationID(jobID);
    response.setJMSReplyTo(confirmedQueue);
    return response;
  }


  Thread workerThread = null;

  private Destination handleConfirmPickQueueMsg() throws JMSException {
    Queue confirmPickQueue = session.createQueue(config.confirmPickQueue);
    MessageConsumer consumer = session.createConsumer(confirmPickQueue);
    consumer.setMessageListener(m -> {
      log.info("Got confirmation msg for pickedJob: {}", m);
      boolean goAhead = true;
      if (goAhead) {
        // TODO: start JobRunner thread with StatusReporter
        workerThread = new Thread(() -> {
          log.info("Working...");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          acceptingJobs = true;
        }, "JobRunner-");
        workerThread.start();
      } else {
        acceptingJobs = true;
      }
    });
    return confirmPickQueue;
  }

}
