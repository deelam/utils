package net.deelam.vertx;

import java.util.concurrent.CompletableFuture;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public final class VerticleUtils {

  /**
   * Verticles can be deployed in any order and find each other.
   * Given a client and server verticles,
   * - If client deploys first and publishes "I'm here as client"; 
   *   DEPRECATED: server deploys and publishes "I'm here as server and my address is ..."; client uses broadcasted address   
   * - If client deploys second and publishes "I'm here as client with address"; 
   *   server sends "My address is ..." to client; client uses response to register/use/etc
   * 
   * Client needs serviceContactInfo handler for both server's broadcast and reply.
   * Server needs client responder.
   * Both need announcement publisher.
   */

  private static final String YP_ADDRESS_PREFIX = "dl.";

  /**
   * Reminder: call this method after setting up the verticle's consumers to handle incoming messages.
   * @param vertx
   * @param type
   * @param serviceContactInfo
   * @return
   */
  public static CompletableFuture<MessageConsumer<Object>> announceServiceType(Vertx vertx, String type,
      String serviceContactInfo) {
    return announceServiceType(vertx, type, serviceContactInfo, false);
  }

  public static CompletableFuture<MessageConsumer<Object>> announceServiceType(Vertx vertx, String type,
      String serviceContactInfo, boolean exitIfServerExists) {

    CompletableFuture<MessageConsumer<Object>> future = new CompletableFuture<>();
    long time = System.currentTimeMillis();
    setupServerMsgHandlers(vertx, type, serviceContactInfo, time, future);

    if (exitIfServerExists) {
      // detect existence of services listening on the same eventBus topic
      checkForExistingServer(vertx, type, time);

      //Don't need to publish service if AbstractVerticleClient is used: 
      //publishService(vertx, type, serviceContactInfo);
    } else {
      log.warn("DEPRECATED: Use AbstractServerVerticle to announceServiceType instead; "
          + "and use AbstractVerticleClient to find server");
      publishService(vertx, type, serviceContactInfo);
    }
    return future;
  }

  private static void publishService(Vertx vertx, String type, String serviceContactInfo) {
    String listenBroadcastAddr = genServerListenBroadcastAddr(type);
    log.info("Announcing service type={}: {} listening at {}", type, serviceContactInfo, listenBroadcastAddr);
    String publishAddr = genClientListenBroadcastAddr(type);
    log.debug("Publishing service at {}", publishAddr);
    vertx.eventBus().publish(publishAddr, serviceContactInfo); // server's broadcast
  }

  private static final String NONCLIENT = "nonclient.";

  private static void setupServerMsgHandlers(Vertx vertx, String type, String serviceContactInfo, long myTime,
      CompletableFuture<MessageConsumer<Object>> future) {
    // in response to client's broadcast, notify that particular client (Vertx does not allow msg.reply())
    String listenBroadcastAddr = genServerListenBroadcastAddr(type);
    log.info("Setting up msgHandlers for server {} type={} at " + listenBroadcastAddr, serviceContactInfo, type);
    MessageConsumer<Object> consumer = vertx.eventBus().consumer(listenBroadcastAddr, msg -> {
      String clientAddress = (String) msg.body();
      log.debug("Got {} client registration from {}", type, clientAddress);
      if (clientAddress.startsWith(NONCLIENT)) {
        long serverStartTime = Long.parseLong(clientAddress.substring(NONCLIENT.length()));
        if (serverStartTime > myTime)
          notifyOtherServersToShutdown(vertx, myTime, listenBroadcastAddr);
        else if (serverStartTime < myTime)
          shutdownJvm();
      } else
        connectWithClient(vertx, serviceContactInfo, clientAddress);
    });

    future.complete(consumer);
  }

  private static void checkForExistingServer(Vertx vertx, String type, long myTime) {
    String listenBroadcastAddr = genServerListenBroadcastAddr(type);
    // act like a client to check for existing server
    String checkAddr = NONCLIENT + myTime;
    log.info("Checking for existing server at {} with checkAddr={} ...", listenBroadcastAddr, checkAddr);
    vertx.eventBus().publish(listenBroadcastAddr, checkAddr);
  }

  private static void notifyOtherServersToShutdown(Vertx vertx, long myTime, String listenBroadcastAddr) {
    String checkAddr = NONCLIENT + myTime;
    log.info("Cause other server to shutdown {} with my checkAddr={} ...", listenBroadcastAddr, checkAddr);
    vertx.eventBus().publish(listenBroadcastAddr, checkAddr);
  }

  private static void shutdownJvm() {
    //StartVertx.shutdown();
    log.error("Other Vertx applications may have been corrupted; please restart them!");
    // If there is more than 1 Ignite 'server' when this occurs, Vertx messages
    // are not routed correctly -- "No handlers for address".
    System.exit(320);
  }

  private static final Object OK_REPLY = "ACK";

  private static String genClientListenBroadcastAddr(String type) {
    return YP_ADDRESS_PREFIX + "clientBroadcastInbox." + type;
  }

  private static String genServerListenBroadcastAddr(String type) {
    return YP_ADDRESS_PREFIX + "serverBroadcastInbox." + type;
  }

  private static void connectWithClient(Vertx vertx, String serviceContactInfo, String clientAddress) {
    vertx.eventBus().send(clientAddress, serviceContactInfo, clientResp -> {
      if (clientResp.failed()) {
        log.info("Didn't get ACK from client={}; retrying", clientAddress /*, clientResp.cause()*/);
        // to address Vertx problem of not sending response to ALL clients
        connectWithClient(vertx, serviceContactInfo, clientAddress); //retry
      }
    });
  }

  static int clientCount = 0;

  /**
   * 
   * @param vertx
   * @param serviceType
   * @param serverRespHandler may be called more than once for the same server
   * @return client's inbox event bus address
   */
  public synchronized static String announceClientType(Vertx vertx, String serviceType,
      Handler<Message<String>> serverRespHandler) {
    String clientListenBroadcastAddr = genClientListenBroadcastAddr(serviceType);
    log.info("Announcing client of serviceType={}: listening at {} with handler={}", serviceType,
        clientListenBroadcastAddr, serverRespHandler);
    vertx.eventBus().consumer(clientListenBroadcastAddr, (Message<String> msg) -> {
      log.debug("Got server broadcast: serverAddr={}", msg.body());
      serverRespHandler.handle(msg);
    }); // handle server's broadcast

    String myAddress =
        YP_ADDRESS_PREFIX + serviceType + ".clientInbox_" + (++clientCount) + "_" + System.currentTimeMillis();
    vertx.eventBus().consumer(myAddress, (Message<String> msg) -> {
      log.debug("{}: Got server reply: {}", myAddress, msg.body());
      serverRespHandler.handle(msg);
      msg.reply(OK_REPLY);
    }); // handle server response to client's broadcast

    reannounceClientType(vertx, serviceType, myAddress);
    return myAddress;
  }

  public synchronized static void reannounceClientType(Vertx vertx, String serviceType, String myAddress) {
    String serverListenBroadcastAddr = genServerListenBroadcastAddr(serviceType);
    log.debug("Publishing client address={} for serviceType={} to {}", myAddress, serviceType,
        serverListenBroadcastAddr);
    vertx.eventBus().publish(serverListenBroadcastAddr, myAddress); // client's broadcast
  }

}

