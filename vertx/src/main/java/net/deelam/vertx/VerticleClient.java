package net.deelam.vertx;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.rpc.ServiceWaiter;

/**
 * To be used with AbstractServerVerticle
 *
 */
@Slf4j
@ToString
public class VerticleClient {
  private final String serviceType;
  private final Vertx vertx;

  public VerticleClient(Vertx vertx, String serviceType) {
    super();
    this.serviceType = serviceType;
    this.vertx = vertx;
    start(Collections.emptySet());
  }

  public VerticleClient(Vertx vertx, String serviceType, Set<Class<?>> msgBeans) {
    super();
    this.serviceType = serviceType;
    this.vertx = vertx;
    start(msgBeans);
    
    waiter=new ServiceWaiter(vertx, serviceType);
    waiter.listenAndBroadcast();
  }

  @Getter(lazy=true)
  private final String svcAddrPrefix = waitUntilReady();
  private ServiceWaiter waiter;  
  private String waitUntilReady() {
    return waiter.awaitServiceAddress();
  }

  private void start(Set<Class<?>> msgBeans) {
    msgBeans.forEach(beanClass -> KryoMessageCodec.register(vertx.eventBus(), beanClass));
  }

  public <T> Future<T> notify(Enum<?> method, T msg) {
    return notify(method.name(), msg);
  }

  /**
   * Call a void-returning method on the service with arguments.
   * Can wait on response by calling Future.get() on the returned Future
   * @param method to be called on service
   * @param argsObj to be passed to method
   * @return Future containing the sent argsObj
   */
  public <T> Future<T> notify(String method, T argsObj) {
    CompletableFuture<T> future = new CompletableFuture<>();
    waiter.vertxContext().runOnContext((v) -> {
      log.info("Sending msg to {}: {}", getSvcAddrPrefix() + method, argsObj);
      vertx.eventBus().send(getSvcAddrPrefix() + method, argsObj, (AsyncResult<Message<T>> resp) -> {
        if (resp.failed()) {
          log.error("Message failed: msg=" + argsObj, resp.cause());
          future.completeExceptionally(resp.cause());
        } else if (resp.succeeded()) {
          log.debug("send response={}", resp.result().body());
          future.complete(argsObj);
        }
      });
    });
    return future;
  }

  public <T, R> Future<R> query(Enum<?> method, T msg) {
    return query(method.name(), msg);
  }

  /**
   * Call an object-returning method on the service with arguments.
   * Get the result object (of the method) by calling Future.get() on the returned Future
   * @param method to be called on service
   * @param argsObj primitive or JsonArray or JsonObject or Bean (remember to registerMessageBeans()) to be passed to method
   * @return a Future containing a primitive or JsonArray or JsonObject
   */
  public <T, R> Future<R> query(String method, T argsObj) {
    CompletableFuture<R> future = new CompletableFuture<>();
    log.info("Sending query to {}: {}", getSvcAddrPrefix() + method, argsObj);
    vertx.eventBus().send(getSvcAddrPrefix() + method, argsObj, (AsyncResult<Message<R>> resp) -> {
      if (resp.failed()) {
        log.error("Message failed: msg=" + argsObj, resp.cause());
        future.completeExceptionally(resp.cause());
      } else {
        log.debug("query response={}", resp.result().body());
        future.complete(resp.result().body());
      }
    });
    return future;
  }

  @SuppressWarnings("unchecked")
  public static <T> T[] jsonArrayToObjectArray(JsonArray ja, T[] destArray) {
    return (T[]) ja.getList().toArray(destArray);
  }

}
