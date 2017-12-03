package net.deelam.activemq;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.RemoveInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerCounter implements MessageListener {
  static final boolean DEBUG = false;

  @Getter
  int count = 0;

  public ConsumerCounter(Session session, Destination topic) throws JMSException {
    ActiveMQTopic advTopic = AdvisorySupport.getConsumerAdvisoryTopic(topic);
    session.createConsumer(advTopic).setMessageListener(this);
  }

  public void onMessage(Message msg) {
    if (msg instanceof ActiveMQMessage) {
      try {
        if (DEBUG) {
          ActiveMQMessage aMsg = (ActiveMQMessage) msg;
          log.info("header={}", aMsg.getProperties());
          DataStructure ds = aMsg.getDataStructure();
          log.info("{}", ds);
          if (ds instanceof ConsumerInfo) {
            ConsumerInfo cons = (ConsumerInfo) ds;
          } else if (ds instanceof RemoveInfo) {
            RemoveInfo rem = (RemoveInfo) ds;
          }
        }
        count = msg.getIntProperty("consumerCount");
      } catch (Exception e) {
        log.error("Failed to process message: " + msg, e);
      }
    } else {
      log.warn("Unexpected msg class={}", msg.getClass());
    }
  }

}
