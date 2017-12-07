package net.deelam.vertx;

import java.util.UUID;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Use case: only one type of this verticle should exist in clustered Vertx
 * 
 * Typical usage is to create one this for each singleton verticle type
 * in your clustered Vertx, rather than try to wrap or combine this with another verticle.
 * 
 * Verticles broadcast their time to the same broadcast address.
 * The verticle with the earliest time gets to stay; all others shutdown.
 * 
 * For safety, include a unique id in the header in case 2 verticles have the same time.
 * In which case, use compareTo() and the lesser value stays.
 */
@RequiredArgsConstructor
@Slf4j
public class SingletonVerticle extends AbstractVerticle {
  final String type;
  final long time = System.currentTimeMillis();
  final String myId=UUID.randomUUID().toString();
  
  private boolean shutdownCalled=false;
  @Setter
  Runnable shutdown=()->{ // don't need to synchronized b/c running in a verticle
    if(shutdownCalled) return;
    shutdownCalled=true;
    log.error("Shutting down {} ({}) due to another singleton server", time, myId);
    if(true){
      vertx.close();
      new Throwable("Found another existing singleton server!  "
          + "This may have been corrupted other Vertx-based applications").printStackTrace();;
    }else{
      log.error("Other Vertx applications may have been corrupted due to Ignite cluster manager;"
          + " please restart them!");
      // If there is more than 1 Ignite 'server' when this occurs, Vertx messages
      // are not routed correctly -- "No handlers for address".
      System.exit(320);
    }
  };;

  String broadcastInbox;
  
  @Override
  public void start() throws Exception {
    super.start();
    
    // detect existence of services listening on the same eventBus topic
    broadcastInbox = "singletonBroadcastInbox." + type;
    setupBroadcastInboxHandler(time);
    log.info("Broadcast to verticles at {} with my time={} ...", broadcastInbox, time);
    broadcastToOtherVerticles(time);
  }

  private static final String VERTICLE_ID = "verticleID";

  private void setupBroadcastInboxHandler(long myTime) {
    // in response to broadcast msg, notify that particular client (Vertx does not allow msg.reply())
    log.info("Setting up msgHandlers for type={} at {}", type, broadcastInbox);
    vertx.eventBus().consumer(broadcastInbox, msg -> {
      long senderStartTime = (long) msg.body();
      log.debug("Got senderStartTime for {}: {}", type, senderStartTime);
      if (senderStartTime > myTime){
        log.info("Cause other verticles to shut down at {} with my time={} ...", broadcastInbox, time);
        broadcastToOtherVerticles(myTime);
      } else if (senderStartTime < myTime){
        shutdown.run();
      }else { // time is the same, so make sure it came from this verticle
        String senderId= msg.headers().get(VERTICLE_ID);
        int diff=myId.compareTo(senderId);
        if(diff!=0){
          log.info("Unlikely but can occur: same time but different verticles: {} vs {}", myId, senderId);
          if(diff<0){
            log.info("Cause other verticles to shut down at {} with my time={} ...", broadcastInbox, time);
            broadcastToOtherVerticles(myTime);
          } else if(diff>0){
            shutdown.run();
          }
        }
      }
    });
  }

  private void broadcastToOtherVerticles(long myTime) {
    DeliveryOptions opts=new DeliveryOptions().addHeader(VERTICLE_ID, myId);
    vertx.eventBus().publish(broadcastInbox, myTime, opts);
  }

}
