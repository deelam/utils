package net.deelam.activemq.rpc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import org.apache.activemq.command.ConsumerInfo;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.CombinedResponse;
import net.deelam.activemq.TopicUtils;

@Slf4j
public class AmqComponentRegistry {

  public static final String REGISTRY_SUBSCRIBER_TOPIC = "componentRegistryTopic";

  private static final String REGISTER = "registerComponent";

  private final Session session;
  private final TopicUtils topicUtils;
  
  public AmqComponentRegistry(Connection connection) throws JMSException {
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    topicUtils = new TopicUtils(session, session.createTopic(REGISTRY_SUBSCRIBER_TOPIC));
    topicUtils.listenToTopicConsumerResponses(REGISTER, null);
  }
  
  public List<String> waitForComponents(String[] comps)
      throws InterruptedException, ExecutionException {
    List<String> compList = Arrays.asList(comps);
    if(compList.isEmpty())
      return compList;
    
    Set<String> compSet = new HashSet<>(compList);
    HashSet<String> remaining = new HashSet<>(compSet);
    int size = comps.length;
    log.info("AMQ: Waiting for these components: {}", Arrays.toString(comps));
    while (true) {
      boolean timedout=false;
      try {
        waitForNComponents(size).get(5, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        timedout=true;
      }
      List<String> registeredComps = queryComponentNames().get();
      remaining.removeAll(registeredComps);
      if (remaining.isEmpty())
        return registeredComps;
      if(timedout)
        log.warn("AMQ: Waiting for these remaining components: {}", remaining);
      else
        log.info("AMQ: remaining={} currComp={}", remaining, registeredComps);
      size = registeredComps.size() + remaining.size();
      log.info("AMQ: waiting for {} components: {}", size, remaining);
    }
  }
  
  public CompletableFuture<Integer> waitForNComponents(int minNumComponentsExpected){
    CompletableFuture<Integer> componentsF = new CompletableFuture<>();
    int currSubscriberCount = topicUtils.getSubscriberCount();
    log.info("AMQ: currSubscriberCount={}", currSubscriberCount);
    if(currSubscriberCount>=minNumComponentsExpected) {
      componentsF.complete(currSubscriberCount);
    } else
      onNewSubscriber(c->{
        int numComponents = topicUtils.getSubscriberCount();
        log.info("AMQ: new component registered: {} {}", numComponents, c);
        if(numComponents>=minNumComponentsExpected) {
          componentsF.complete(numComponents);
        }
      });
    return componentsF;
  }

  public void onNewSubscriber(Consumer<ConsumerInfo> newSubscriberHandler) {
    topicUtils.setNewSubscriberHandler(newSubscriberHandler);
  }
  
  public void shutdown() throws IOException {
    topicUtils.close();
  }
  
  static final String COMMAND = "COMMAND";
  static final int GET_NAMES = 1;
  static final int GET_ATTRIBS = 2;

  public CompletableFuture<List<String>> queryComponentNames() {
    log.info("queryOperations");
    try {
      Message message = session.createTextMessage();
      message.setIntProperty(COMMAND, GET_NAMES);
      CombinedResponse<String> cResp =
          topicUtils.queryAsync(REGISTER, message);
      return cResp.getFuture();
    } catch (JMSException e) {
      log.error("When queryComponentNames: ", e);
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
  }
  
  public CompletableFuture<List<Map<String,Object>>> queryComponentAttributes() {
    log.info("queryOperations");
    try {
      Message message = session.createTextMessage();
      message.setIntProperty(COMMAND, GET_ATTRIBS);
      CombinedResponse<Map<String,Object>> cResp =
          topicUtils.queryAsync(REGISTER, message);
      return cResp.getFuture();
    } catch (JMSException e) {
      log.error("When queryComponentAttributes: ", e);
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
  }
}
