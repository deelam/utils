package net.deelam.vertx.rpc;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * To be used with RpcVerticleClient.
 */
@RequiredArgsConstructor
@Slf4j
public class RpcVerticleServer {
  final Vertx vertx;
  final String serversBroadcastAddr;

  public <T> RpcVerticleServer start(String serverAddr, T service) {
    return start(serverAddr, service, true);
  }
  public <T> RpcVerticleServer start(String serverAddr, T service, boolean withDebugHook) {
    VertxRpcUtil rpc=new VertxRpcUtil(vertx, serverAddr);
    if (withDebugHook)
      rpc.setHook(new DebugRpcHook(service.getClass().getSimpleName()));
    log.debug("Registering RPC service at {}: {}", serverAddr, service);
    rpc.registerServer(service);

    if(serversBroadcastAddr!=null)
      vertx.eventBus().consumer(serversBroadcastAddr, (Message<String> clientAddr) -> {
        log.info("{} got client broadcast from {}", serverAddr, clientAddr.body());
        vertx.eventBus().send(clientAddr.body(), serverAddr);
      });
    return this;
  }

}
