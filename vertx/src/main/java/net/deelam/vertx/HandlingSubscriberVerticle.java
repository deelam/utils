package net.deelam.vertx;

import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * Use this class to communicate with the BroadcastingRegistryVerticle.
 *
 */
@Slf4j
public abstract class HandlingSubscriberVerticle extends SubscriberVerticle {

  public HandlingSubscriberVerticle(String type, String serviceAddr) {
    super(type, serviceAddr);
  }

  ///

  @FunctionalInterface
  public static interface ThrowingQueryHandler<B, R> {
    R handle(B body) throws Throwable;
  }

  protected <T, R> MessageConsumer<T> registerMsgHandler(String methodName,
      HandlingSubscriberVerticle.ThrowingQueryHandler<T, R> function) {
    MessageConsumer<T> msgConsumer = vertx.eventBus().consumer(serviceAddr + methodName, (msg) -> {
      try {
        log.debug("{} received query msg: {}", serviceAddr, msg.body());
        R respObj = function.handle(msg.body());
        msg.reply(respObj);
      } catch (Throwable e) {
        log.error("Error when answering '" + methodName + "' query: ", e);
        msg.fail(-11, "Error when answering '" + methodName + "' query: " + e.getMessage());
      }
    });
    //msgConsumers.add(msgConsumer);
    return msgConsumer;
  }

  @FunctionalInterface
  public static interface ThrowingNotificationHandler<B> {
    void handle(B body) throws Exception;
  }

  protected <T> MessageConsumer<T> registerMsgHandler(String methodName,
      HandlingSubscriberVerticle.ThrowingNotificationHandler<T> consumer) {
    log.info("Registering msgHandler to " + serviceAddr + methodName);
    MessageConsumer<T> msgConsumer = vertx.eventBus().consumer(serviceAddr + methodName, (msg) -> {
      try {
        log.debug("{} received query msg method={} with args={}", serviceAddr, methodName, msg.body());
        consumer.handle(msg.body());
        replyAckToAsyncClientMsg(msg);
      } catch (Throwable e) {
        log.error("Error when processing '" + methodName + "' notification: ", e);
        msg.fail(-12, "Error when processing '" + methodName + "' notification: " + e.getMessage());
      }
    });
    //msgConsumers.add(msgConsumer);
    return msgConsumer;
  }

  private static final Object OK_REPLY = "ACK";

  // always reply to client's requests; this is used to let client know the request was received
  protected void replyAckToAsyncClientMsg(Message<?> asyncMsg) {
    asyncMsg.reply(OK_REPLY);
  }

}
