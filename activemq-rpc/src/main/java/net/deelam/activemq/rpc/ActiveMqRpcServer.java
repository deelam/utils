package net.deelam.activemq.rpc;

import javax.jms.JMSException;
import javax.jms.Session;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.MQClient;

/**
 * To be used with RpcVerticleClient.
 */
@RequiredArgsConstructor
@Slf4j
public class ActiveMqRpcServer {
  //final String serversBroadcastAddr;
  private final String brokerURL;

  public <T> void start(String serverAddr, T service) {
    start(serverAddr, service, true);
  }

  public <T> void start(String serverAddr, T service, boolean withDebugHook) {
    try {
      Session session = MQClient.createRpcSession(brokerURL);
      ActiveMqRpcUtil rpc = new ActiveMqRpcUtil(session, serverAddr);
      if (withDebugHook)
        rpc.setHook(new ActiveMqRpcUtil.DebugRpcHook());
      log.info("Creating RPC service for {}", service.getClass().getSimpleName());
      rpc.registerServer(service);

      //    log.info("RpcVerticleServer listening at address={}", serversBroadcastAddr);
      //    vertx.eventBus().consumer(serversBroadcastAddr, (Message<String> clientAddr) -> {
      //      log.info("Got client broadcast from {}", clientAddr.body());
      //      vertx.eventBus().send(clientAddr.body(), serverAddr);
      //    });
    } catch (JMSException e) {
      throw new RuntimeException(e);
    }
    //return this;
  }

}
