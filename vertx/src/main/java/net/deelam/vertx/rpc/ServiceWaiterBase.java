package net.deelam.vertx.rpc;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
@RequiredArgsConstructor
public class ServiceWaiterBase {
  final Vertx vertx;
  final String serversBroadcastAddr;
  
  /**
   * Creates msg consumer assuming reply is a serverAddr string.
   */
  public String createMsgConsumer(CompletableFuture<String> serverAddrF, Consumer<Message<String>> gotServiceAddrHook) {
    return createServerMsgConsumer((String msgBody) -> {
      if (serverAddrF.isDone())
        log.warn("VERTX: Already got serverAddr for service={}, ignoring body={}", serversBroadcastAddr, msgBody);
      else {
        log.info("VERTX: Got serverAddr={} for service={}", msgBody, serversBroadcastAddr);
        serverAddrF.complete(msgBody);
      }
    }, gotServiceAddrHook);
  }

  private MessageConsumer<?> consumer;
  private String myAddress = UUID.randomUUID().toString();

  @Getter
  private Context vertxContext;
  /**
   * Creates msg consumer with given registerServer
   * @param registerServer
   */
  public <T> String createServerMsgConsumer(Consumer<T> registerServer, Consumer<Message<T>> gotServiceAddrHook) {
    vertxContext = vertx.getOrCreateContext();
    if (consumer != null)
      consumer.unregister();
    consumer = vertx.eventBus().consumer(myAddress, (Message<T> msg) -> {
      log.debug("VERTX: Got response from server: {} resulting from broadcast to {}", msg.body(), serversBroadcastAddr);
      registerServer.accept(msg.body());
      if(gotServiceAddrHook!=null) 
        gotServiceAddrHook.accept(msg);
    });
    return myAddress;
  }

  public void unregisterMsgConsumer() {
    consumer.unregister();
    consumer = null;
  }

  public void broadcastServerSearch(CompletableFuture<String> serverAddrF) {
    if (consumer == null)
      throw new IllegalStateException("Must createServerMsgConsumer() before broadcastServerSearch()");
    if (serversBroadcastAddr==null || serversBroadcastAddr.length()==0)
      throw new IllegalStateException("serversBroadcastAddr="+serversBroadcastAddr);
    Handler<Long> broadcastUntilSuccess =
        createBroadcastUntilSuccess(serversBroadcastAddr, myAddress, serverAddrF);
    broadcastUntilSuccess.handle(0L);
  }

  @Setter
  private long broadcastPeriodInSeconds = 3;

  private Handler<Long> createBroadcastUntilSuccess(String serversBroadcastAddr, final String myAddr,
      CompletableFuture<String> serverAddrF) {
    return (time) -> {
      if (!serverAddrF.isDone()) {
        vertxContext.runOnContext((b)->{
          log.info("VERTX: broadcastServerSearch from={} to addr={};  waiting for server response ...", myAddr,
              serversBroadcastAddr);
          vertx.eventBus().publish(serversBroadcastAddr, myAddr);
          // check again later
          vertx.setTimer(broadcastPeriodInSeconds * 1000,
              createBroadcastUntilSuccess(serversBroadcastAddr, myAddr, serverAddrF));
        });
      }
    };
  }
}
