package net.deelam.vertx.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public class ServiceWaiter {

  final ServiceWaiterBase base;
  
  public Context vertxContext(){
    return base.vertxContext();
  }

  public ServiceWaiter(Vertx vertx, String serversBroadcastAddr) {
    base = new ServiceWaiterBase(vertx, serversBroadcastAddr);
  }

  public CompletableFuture<String> listenAndBroadcast() {
    return listenAndBroadcast(null);
  }
  
  public <T> CompletableFuture<String> listenAndBroadcast(Consumer<Message<String>> gotServiceAddrHook) {
    serverAddrF = new CompletableFuture<>();
    base.createMsgConsumer(serverAddrF, gotServiceAddrHook);
    base.broadcastServerSearch(serverAddrF);
    return serverAddrF;
  }

  CompletableFuture<String> serverAddrF;

  public String awaitServiceAddress() {
    if (serverAddrF == null)
      throw new IllegalStateException("Call listenAndBroadCast() first!");
    while (true)
      try {
        if(!serverAddrF.isDone())
          log.info("VERTX: Waiting for server response from '{}' ...", base.serversBroadcastAddr);
        String serverAddr = serverAddrF.get(10, TimeUnit.SECONDS); // wait for serverAddr
        //log.debug("Got server address: {}", serverAddr);
        return serverAddr;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log.warn("VERTX: Retrying to get serverAddr from "+base.serversBroadcastAddr, e);
      }
  }

}
