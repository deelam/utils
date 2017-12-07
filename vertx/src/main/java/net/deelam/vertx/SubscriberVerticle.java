package net.deelam.vertx;

import io.vertx.core.AbstractVerticle;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * Use this class to communicate with the RegistryVerticle or BroadcasterVerticle.
 *
 */
@RequiredArgsConstructor
@Slf4j
public abstract class SubscriberVerticle extends AbstractVerticle {
  final String type;
  final String serviceAddr;
  
  /**
   * Do not call this in the constructor, where vertx is still null.
   * @param msgBeans
   */
  public void registerMsgBeans(Class<?>... msgBeans) {
    for(Class<?> msgBodyClass:msgBeans)
      KryoMessageCodec.register(vertx.eventBus(), msgBodyClass);
  }

  public void start() throws Exception {
    registerMsgHandlers();
    createPublishedMessageHandler();
    publishService();
  }

  abstract public void registerMsgHandlers();

  private void publishService() {
    String publishAddr = RegistryVerticle.genRegistryPublishInbox(type);
    log.info("Announcing service type={} with serviceAddr={} to clients at {}", type, serviceAddr, publishAddr);
    vertx.eventBus().publish(publishAddr, serviceAddr); // Subscriber's broadcast
  }

  static String genSubscriberPublishInbox(String type) {
    return "subscriberInbox." + type;
  }

  private void createPublishedMessageHandler() {
    // in response to client's broadcast, notify that particular client (Vertx does not allow msg.reply())
    String publishInbox = SubscriberVerticle.genSubscriberPublishInbox(type);
    log.info("Setting up msgHandlers for Subscriber {} type={} at " + publishInbox, serviceAddr, type);
    vertx.eventBus().consumer(publishInbox, msg -> {
      String clientAddress = (String) msg.body();
      log.debug("Got {} client registration from {}", type, clientAddress);
      connectWithClient(clientAddress);
    });
  }

  private void connectWithClient(String clientAddress) {
    vertx.eventBus().send(clientAddress, serviceAddr, clientResp -> {
      if (clientResp.failed()) {
        log.info("Reply from client={} failed: {}; retrying ", clientAddress, clientResp.cause().getMessage());
        // to address Vertx problem of not sending response to ALL clients
        connectWithClient(clientAddress); //retry
      }
    });
  }

}
