package net.deelam.vertx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * To be used with VerticleClient
 */
@Slf4j
@RequiredArgsConstructor
@ToString
public abstract class AbstractServerVerticle extends AbstractVerticle {
  private final String serviceType;
  private final String inboxAddressBase;

  @Setter
  private Set<Class<?>> msgBeans=Collections.emptySet();
  
  @Override
  public void start() throws Exception {
    msgBeans.forEach( beanClass -> KryoMessageCodec.register(vertx.eventBus(), beanClass));
    registerMsgHandlers();
    // announce after registerMsgHandlers(); otherwise no handlers to respond to client
    VerticleUtils.announceServiceType(vertx, serviceType, inboxAddressBase, true);
    log.info("Ready: address={} this={}", inboxAddressBase, this);
  }

  abstract protected void registerMsgHandlers();
  
  protected List<MessageConsumer<?>> msgConsumers=new ArrayList<>();
  @Override
  public void stop() throws Exception {
    msgConsumers.forEach((c)->{
      log.info("Unregistering {}",c);
      c.unregister();
    });
    super.stop();
  }
  
  ///
  
  @FunctionalInterface
  public static interface ThrowingQueryHandler<B,R> {
    R handle(B body) throws Throwable;
  }

  protected <T,R> MessageConsumer<T> registerMsgHandler(Enum<?> methodName, ThrowingQueryHandler<T,R> function) {
    return registerMsgHandler(methodName.name(), function);
  }
  protected <T,R> MessageConsumer<T> registerMsgHandler(String methodName, ThrowingQueryHandler<T,R> function) {
    MessageConsumer<T> msgConsumer = vertx.eventBus().consumer(inboxAddressBase + methodName, (msg)->{
      try {
        log.debug("{} received query msg: {}", serviceType, msg.body());
        R respObj = function.handle(msg.body());
        msg.reply(respObj);
      } catch (Throwable e) {
        log.error("Error when answering '"+methodName+"' query: ", e);
        msg.fail(-11, "Error when answering '"+methodName+"' query: "+e.getMessage());
      }
    });
    msgConsumers.add(msgConsumer);
    return msgConsumer;
  }
  
  @FunctionalInterface
  public static interface ThrowingNotificationHandler<B> {
    void handle(B body) throws Exception;
  }
  
  protected <T> MessageConsumer<T> registerMsgHandler(Enum<?> methodName, ThrowingNotificationHandler<T> consumer) {
    return registerMsgHandler(methodName.name(), consumer);
  }
  protected <T> MessageConsumer<T> registerMsgHandler(String methodName, ThrowingNotificationHandler<T> consumer) {
    log.info("Registering msgHandler to "+inboxAddressBase + methodName);
    MessageConsumer<T> msgConsumer = vertx.eventBus().consumer(inboxAddressBase + methodName, (msg)->{
      try{
        log.debug("{} received msg: {}", serviceType, msg.body());
        consumer.handle(msg.body());
        replyAckToAsyncClientMsg(msg);
      } catch (Throwable e) {
        log.error("Error when processing '"+methodName+"' notification: ", e);
        msg.fail(-12, "Error when processing '"+methodName+"' notification: "+e.getMessage());
      }
    });
    msgConsumers.add(msgConsumer);
    return msgConsumer;
  }
  
  private static final Object OK_REPLY = "ACK";
  
  // always reply to client's requests; this is used to let client know the request was received
  protected void replyAckToAsyncClientMsg(Message<?> asyncMsg) {
    asyncMsg.reply(OK_REPLY);
  }
}
