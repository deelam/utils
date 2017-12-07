package net.deelam.vertx.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.vertx.core.Vertx;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
//import lombok.extern.slf4j.Slf4j;

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
//@Slf4j
public class RpcVerticleClient {
  
  final Vertx vertx;
  
  @Getter
  final ServiceWaiter waiter;
  
  public RpcVerticleClient(Vertx vertx, String serversBroadcastAddr){
    this.vertx=vertx;
    waiter=new ServiceWaiter(vertx, serversBroadcastAddr);
  }

  CompletableFuture<String> serverAddrF;

  public RpcVerticleClient start() {
    serverAddrF = waiter.listenAndBroadcast();
    return this;
  }

  public <T> Supplier<T> invalidateAndFindNewServer(Class<T> clazz) {
    serverAddrF = waiter.listenAndBroadcast();
    return createRpcClient(clazz);
  }

  /**
   * Creates RPC client, blocking until serverAddr received from server
   * @param clazz
   * @return proxy for server
   */
  public <T> Supplier<T> createRpcClient(Class<T> clazz) {
    return createRpcClientSupplier(clazz, true);
  }

  /**
   * Creates RPC client supplier.  
   * Calling Supplier.get() will block until serverAddr received from server
   * @param clazz
   * @param withDebugHook whether to add a hook that logs RPC calls
   * @return proxy for server
   */
  public <T> Supplier<T> createRpcClientSupplier(Class<T> clazz, boolean withDebugHook) {
    if (serverAddrF == null)
      throw new IllegalStateException("Run start() first.");
    return ()->createRpcClient(serverAddrF, clazz, withDebugHook);
  }

  /**
   * Creates RPC client at serverAddr
   * @param serverAddrF2
   * @param clazz
   * @return proxy for server
   */
  <T> T createRpcClient(CompletableFuture<String> serverAddrF, Class<T> clazz, boolean withDebugHook) {
    String serverAddr = waiter.awaitServiceAddress();
    VertxRpcUtil rpc=new VertxRpcUtil(vertx, serverAddr);
    if (withDebugHook)
      rpc.setHook(new DebugRpcHook(clazz.getSimpleName()));
    return rpc.createClient(clazz);
  }

}
