package net.deelam.vertx.rpc;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.ClusteredVertxConfig;
import net.deelam.vertx.SingletonVerticle;
import net.deelam.vertx.StartVertx;

@Slf4j
public class SingletonVerticleTest {
  private static String myServiceType = "myServiceType";

  public Vertx createClusteredVertx(boolean isServer) throws InterruptedException, ExecutionException {
    CompletableFuture<Vertx> v = new CompletableFuture<>();
    ClusteredVertxConfig config=new ClusteredVertxConfig();
    StartVertx.createClustered(config, (vertx) -> v.complete(vertx));
    return v.get();
  }

  Vertx vertx;

  @Before
  public void before() throws InterruptedException, ExecutionException {
    vertx = createClusteredVertx(true);
  }

  @After
  public void after() {
    vertx.close();
  }

  @Test
  public void test1Server() throws InterruptedException, ExecutionException {
    vertx.deployVerticle(new SingletonVerticle(myServiceType));
    log.info("Sleeping");
    Thread.sleep(6000);
    assertEquals(1, vertx.deploymentIDs().size());
  }

  @Test
  public void test2Servers() throws InterruptedException, ExecutionException {
    vertx.deployVerticle(new SingletonVerticle(myServiceType));
    log.info("Sleeping");
    Thread.sleep(1000);
    
    Vertx vertx2 = createClusteredVertx(false);
    log.info("------- Expecting stacktrace to be shown soon; don't worry");
    vertx2.deployVerticle(new SingletonVerticle(myServiceType));

    Thread.sleep(5000);
    assertEquals(1, vertx.deploymentIDs().size());
    assertEquals(0, vertx2.deploymentIDs().size());
  }

}
