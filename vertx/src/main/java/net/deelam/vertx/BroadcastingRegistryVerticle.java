package net.deelam.vertx;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * Use this class to communicate with the *SubscriberVerticle.
 * 
 * See RegistryVerticle javadoc.
 * 
 */
@Slf4j
public class BroadcastingRegistryVerticle extends RegistryVerticle {

  public BroadcastingRegistryVerticle(String serviceType) {
    super(serviceType);
  }

  private Context eventLoopContext;

  @Override
  public void start() throws Exception {
    super.start();
    eventLoopContext = context;
  }
  ///

  /**
   * Call a void-returning method on the subscribers with given arguments.
   * Can wait on response by calling CountDownLatch.await() on the returned object.
   * @param method to be called on service
   * @param argsObj to be passed to method
   * @return CountDownLatch 
   */
  public <T> CountDownLatch notify(String method, T argsObj) {
    //threadPool.execute(()->{

    CountDownLatch latch = new CountDownLatch(subscriberAddresses.size());
    for (String addrPrefix : subscriberAddresses) {
      log.info("Sending msg to {}: {}", addrPrefix + method, argsObj);
      vertx.eventBus().send(addrPrefix + method, argsObj, (AsyncResult<Message<T>> resp) -> {
        if (resp.failed()) {
          log.error("Message failed: msg=" + argsObj, resp.cause());
        } else {
          log.debug("  from {}: ack reply={}", addrPrefix, resp.result().body());
        }
        latch.countDown();
      });
    }
    return latch;
  }

  /**
   * Call an object-returning method on the subscribers with given arguments.
   * Get the result object (of the method) by calling Future.get() on the returned Future
   * @param method to be called on service
   * @param argsObj primitive or JsonArray or JsonObject or Bean to be passed to method
   * @return a Future containing a primitive or JsonArray or JsonObject or Bean
   */
  public <T, R> CompletableFuture<Set<R>> query(String method, T argsObj) {
    CompletableFuture<Set<R>> future = new CompletableFuture<>();
    eventLoopContext.runOnContext((v) -> {
      CountDownLatch latch = new CountDownLatch(subscriberAddresses.size());
      Set<R> returnedObjs = new HashSet<>();
      for (String svcAddrPrefix : subscriberAddresses) {
        log.info("Sending query to {}: {}", svcAddrPrefix + method, argsObj);
        vertx.eventBus().send(svcAddrPrefix + method, argsObj, (AsyncResult<Message<R>> resp) -> {
          if (resp.failed()) {
            log.error("Message failed: msg=" + argsObj, resp.cause());
          } else {
            log.debug("  from {}: query response={}", svcAddrPrefix, resp.result().body());
            returnedObjs.add(resp.result().body());
          }
          latch.countDown();
          if (latch.getCount() == 0)
            future.complete(returnedObjs);
        });
      }
    });
    return future;
  }

}
