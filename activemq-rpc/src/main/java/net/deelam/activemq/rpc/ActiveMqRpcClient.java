package net.deelam.activemq.rpc;

import java.util.UUID;
import javax.jms.JMSException;
import javax.jms.Session;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.MQClient;

/**
 *  Use case: Client initiates call to Server and will wait until Server is available.
 *  
 *  Client will repeatedly broadcast a connect request message until 
 *  it gets response from at least one Server(s).  
 *  It keeps 1 (or all) addresses from responses to make calls.  
 *  Client has a message consumer on a unique address to listen for server responses.
 *  Server has a consumer on the broadcast address to listen for client connection requests.
 *  Server doesn't need to announce (broadcast) on startup.  
 *  Server responds to Client's broadcast with a notify, not waiting for an ACK reply (client can re-broadcast).
 *  If Server goes down (or client call fails), client broadcast for another Server and re-calls.
 *  
 *  Use vertx-rpc to make calls, catching exceptions to retry (with another server).
 */
@Accessors(fluent = true)
@RequiredArgsConstructor
@Slf4j
public class ActiveMqRpcClient {
  final String serversBroadcastAddr;
  private final String brokerURL;
  final String myAddress = UUID.randomUUID().toString();

  /**
   * Creates RPC client to serverAddr
   * @param serverAddr
   * @param clazz
   * @return proxy for server
   */
  <T> T createRpcClient(String serverAddr, Class<T> iface, boolean withDebugHook) {
    try {
      Session session = MQClient.createRpcSession(brokerURL);
      ActiveMqRpcUtil rpc = new ActiveMqRpcUtil(session, serverAddr);
      if (withDebugHook)
        rpc.setHook(new ActiveMqRpcUtil.DebugRpcHook());
      log.info("Creating RPC client for {}", iface.getSimpleName());
      return rpc.createClient(iface);
    } catch (JMSException e) {
      throw new RuntimeException(e);
    }
  }

}
