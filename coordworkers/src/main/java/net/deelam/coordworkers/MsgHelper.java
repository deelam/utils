package net.deelam.coordworkers;

import javax.jms.Session;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.MQClient;
import net.deelam.activemq.MQClient.MsgHandler;

@Slf4j
public final class MsgHelper {
  private MsgHelper() {}

  static void listenToJobStatusMsgs(Session session, String stateTopicName, String doneTopicName,
      String failedTopicName) {
    MsgHandler listener = message -> {
      if (message instanceof TextMessage) {
        TextMessage txtMessage = (TextMessage) message;
        String currMsg = txtMessage.getText();
        log.info("Job statusMessage received: " + currMsg);
      } else {
        log.error("Invalid statusMessage received: {}", message);
      }
    };
    if (stateTopicName != null)
      MQClient.createTopicConsumer(session, stateTopicName, listener);
    if (doneTopicName != null)
      MQClient.createTopicConsumer(session, doneTopicName, listener);
    if (failedTopicName != null)
      MQClient.createTopicConsumer(session, failedTopicName, listener);
  }

}
