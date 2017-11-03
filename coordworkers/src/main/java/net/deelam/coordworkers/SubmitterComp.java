package net.deelam.coordworkers;

import java.util.Properties;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.Constants;
import net.deelam.activemq.MQClient;
import net.deelam.zkbasedinit.ComponentI;

@Slf4j
public class SubmitterComp implements ComponentI {

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
    final int deliveryMode = DeliveryMode.NON_PERSISTENT;

    // populate and print remaining unused properties
    public SubmitterCompConfig(Properties props) {
      super(props);
      brokerUrl = Constants.getTcpBrokerUrl(useRequiredRefProperty(props, "brokerUrl.ref"));
      submitJobQueue = useRequiredProperty(props, "msgQ.submitJob");
      jobStateTopic = useRequiredProperty(props, "msgT.jobState");
      jobDoneTopic = useRequiredProperty(props, "msgT.jobDone");
      jobFailedTopic = useRequiredProperty(props, "msgT.jobFailed");
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

      MessageProducer jobSubmitterMP =
          MQClient.createQueueMsgSender(session, config.submitJobQueue, config.deliveryMode);

      MsgHelper.listenToJobStatusMsgs(session, config.jobStateTopic, config.jobDoneTopic, null);
      MQClient.createTopicConsumer(session, config.jobFailedTopic, message -> {
        if (message instanceof TextMessage) {
          TextMessage txtMessage = (TextMessage) message;
          String currMsg = txtMessage.getText();
          log.info("Submitting repeat job due to: {}", currMsg);
          sendSubmitJobMsg(jobSubmitterMP);
        } else {
          log.error("Invalid statusMessage received: {}", message);
        }
      });

      new Thread(() -> {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        sendSubmitJobMsg(jobSubmitterMP);
      }, "submitJob").start();

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

  private void sendSubmitJobMsg(MessageProducer jobSubmitterMP) {
    try {
      Message message = session.createTextMessage("submit Job 123");
      jobSubmitterMP.send(message);
    } catch (JMSException e) {
      log.error("When sending message on: {}", config.submitJobQueue, e);
    }
  }

}
