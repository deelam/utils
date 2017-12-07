package net.deelam.vertx;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Use this class to communicate with the *SubscriberVerticle.
 * 
 * Use case: Registry needs addresses of ALL Servers and Registry doesn't want to wait. 
 *  
 * Also new Subscribers are immediately known to clients.
 * Registrys and Subscribers announce (broadcast) their unique address on startup.
 * Registry has consumer listening for Subscriber broadcast announcement, 
 *     which registers Subscriber's address.
 * Subscriber has consumer listening for client broadcast announcement, 
 *     which sends its address to the client, who replies with an ACK 
 * (sometimes needed Vertx doesn't seem to send msg to Registry -- for some reason!)
 */
@RequiredArgsConstructor
@Slf4j
public class RegistryVerticle extends AbstractVerticle {
  final String serviceType;
  
  @Getter
  final Set<String> subscriberAddresses = new HashSet<>();

  /**
   * Do not call this in the constructor, where vertx is still null.
   * @param msgBeans
   */
  public void registerMsgBeans(Class<?>... msgBeans) {
    for(Class<?> msgBodyClass:msgBeans)
      KryoMessageCodec.register(vertx.eventBus(), msgBodyClass);
  }


  @Override
  public void start() throws Exception {
    super.start();
    
    Handler<Message<String>> subscriberRespHandler = (Message<String> msg) -> {
      subscriberAddresses.add(msg.body());
      log.info("Added new subscriber: subscriberAddrs={}", subscriberAddresses);
      subscriberAdded(msg.body());
    };
    
    createSubscriberBroadcastConsumer(subscriberRespHandler);
    createConsumerAndAnnounce(subscriberRespHandler);
    
    broadcastConnectRequest();
  }
  
  protected void subscriberAdded(String subscriberAddr) {
  }

  public void invalidateAndRebroadcast() {
    subscriberAddresses.clear();
    broadcastConnectRequest();
  }

  static String genRegistryPublishInbox(String type) {
    return "registryInbox." + type;
  }

  void createSubscriberBroadcastConsumer(Handler<Message<String>> subscriberRespHandler) {
    String publishInbox = genRegistryPublishInbox(serviceType);
    log.info("Setting up msgHandler for Registry type={} at " + publishInbox, serviceType);
    vertx.eventBus().consumer(publishInbox, (Message<String> msg) -> {
      log.debug("Got subscriber broadcast: subscriberAddr={}", msg.body());
      subscriberRespHandler.handle(msg);
    }); // handle subscriber's broadcast
  }

  String myAddress = "registryInbox_" + UUID.randomUUID();

  // only needed because of sometimes needed Vertx doesn't seem to send msg to Registry -- for some reason!
  private void createConsumerAndAnnounce(Handler<Message<String>> subscriberRespHandler) {
    log.info("Setting up subscriberMsgHandler for Registry type={} at " + myAddress, serviceType);
    vertx.eventBus().consumer(myAddress, (Message<String> msg) -> {
      log.debug("{}: Got subscriber reply: {}", myAddress, msg.body());
      subscriberRespHandler.handle(msg);
      msg.reply("ACK");
    }); // handle subscriber response to client's broadcast
  }

  private void broadcastConnectRequest() {
    String subscriberListenBroadcastAddr = SubscriberVerticle.genSubscriberPublishInbox(serviceType);
    log.info("Announcing client address={} for serviceType={} to {}", myAddress, serviceType,
        subscriberListenBroadcastAddr);
    vertx.eventBus().publish(subscriberListenBroadcastAddr, myAddress); // client's broadcast
  }

}
