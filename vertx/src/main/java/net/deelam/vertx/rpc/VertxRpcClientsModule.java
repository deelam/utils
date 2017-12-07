package net.deelam.vertx.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.google.inject.AbstractModule;

import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/// provides verticle clients used by REST services
@RequiredArgsConstructor
@Slf4j
public class VertxRpcClientsModule extends AbstractModule {
  final CompletableFuture<Vertx> vertxF;
  Vertx vertx;

  @Override
  protected void configure() {
    try {
      boolean haveToWait = !vertxF.isDone();
      if (haveToWait)
        log.info("VERTX: Waiting for Vertx ...");
      vertx = vertxF.get(10, TimeUnit.SECONDS);
      if (haveToWait)
        log.info("VERTX: Got Vertx");
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  protected boolean debug = false;


  // For Guice, cannot block waiting for server side to come up, so returns a Supplier instead.
  // Also, the thread calling Supplier.get() needs to run in a separate thread such that incoming message is not blocked.
  public <T> Supplier<T> getClientSupplierFor(Class<T> clazz, String serviceType) {
    log.info("VERTX: Creating supplier of RPC client for {}", clazz.getSimpleName());
    Supplier<T> rpcClientS = new RpcVerticleClient(vertx, serviceType).start()
        .createRpcClientSupplier(clazz, debug);
    return rpcClientS;
  }
}
