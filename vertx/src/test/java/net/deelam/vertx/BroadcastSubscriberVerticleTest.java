package net.deelam.vertx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BroadcastSubscriberVerticleTest {

  Vertx vertx;
  private static String myServiceType = "myServiceType";

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  private void createDeployLatch(int vertCount, Consumer<Handler<AsyncResult<String>>> deployF) {
    CountDownLatch latch = new CountDownLatch(vertCount);
    log.info("Before ");

    Handler<AsyncResult<String>> deployHandler = res -> {
      if (res.succeeded()) {
        latch.countDown();
        log.info("countdown={}", latch.getCount());
      } else {
        log.error("Cause: " + res.cause());
      }
    };
    deployF.accept(deployHandler);
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Accessors(fluent = true)
  static class SVerticle extends HandlingSubscriberVerticle {
    public SVerticle(String myServiceAddr) {
      super(myServiceType, myServiceAddr);
    }

    @Setter
    String responsePrefix = "anytime:";

    @Override
    public void registerMsgHandlers() {
      registerMsgHandler("callme1", (args) -> {
        return responsePrefix + args;
      });
    }
  };

  static class BVerticle extends BroadcastingRegistryVerticle {
    public BVerticle() {
      super(myServiceType);
    }
  };

  @Test
  public void test1Server() throws InterruptedException {
    BVerticle reg = new BVerticle();
    createDeployLatch(2, (deployHandler) -> {
      vertx.deployVerticle(new SVerticle("myServiceAddr"), deployHandler);
      vertx.deployVerticle(reg, deployHandler);
    });

    log.info("Sleeping");
    Thread.sleep(1000);
    Set<String> addrs = reg.getSubscriberAddresses();
    assertEquals(1, addrs.size());
  }

  @Test
  public void test1ServerMethodCall() throws InterruptedException, ExecutionException {
    BVerticle reg = new BVerticle();
    createDeployLatch(2, (deployHandler) -> {
      vertx.deployVerticle(new SVerticle("myServiceAddr"), deployHandler);
      vertx.deployVerticle(reg, deployHandler);
    });
    log.info("Sleeping");
    Thread.sleep(1000);
    Set<String> addrs = reg.getSubscriberAddresses();
    assertEquals(1, addrs.size());

    CountDownLatch latch = reg.notify("callme1", "fromRegistry");
    latch.await();

    Future<Set<Object>> res = reg.query("callme1", "fromRegistry");
    assertEquals(1, res.get().size());
    assertTrue(res.get().contains("anytime:fromRegistry"));
  }

  @Test
  public void test2Server() throws InterruptedException {
    BVerticle reg = new BVerticle();
    createDeployLatch(3, (deployHandler) -> {
      vertx.deployVerticle(new SVerticle("myServiceAddr1"), deployHandler);
      vertx.deployVerticle(new SVerticle("myServiceAddr2"), deployHandler);
      vertx.deployVerticle(reg, deployHandler);
    });

    log.info("Sleeping");
    Thread.sleep(1000);
    Set<String> addrs = reg.getSubscriberAddresses();
    assertEquals(2, addrs.size());
  }

  @Test
  public void test2ServerMethodCallsSameResponse() throws InterruptedException, ExecutionException {
    BVerticle reg = new BVerticle();
    createDeployLatch(3, (deployHandler) -> {
      vertx.deployVerticle(new SVerticle("myServiceAddr1"), deployHandler);
      vertx.deployVerticle(new SVerticle("myServiceAddr2"), deployHandler);
      vertx.deployVerticle(reg, deployHandler);
    });

    log.info("Sleeping");
    Thread.sleep(1000);
    Set<String> addrs = reg.getSubscriberAddresses();
    assertEquals(2, addrs.size());

    Future<Set<Object>> res = reg.query("callme1", "fromRegistry");
    assertEquals(1, res.get().size());
    assertTrue(res.get().contains("anytime:fromRegistry"));
  }

  @Test
  public void test2ServerMethodCallsDiffResponse() throws InterruptedException, ExecutionException {
    BVerticle reg = new BVerticle();
    createDeployLatch(3, (deployHandler) -> {
      vertx.deployVerticle(new SVerticle("myServiceAddr1"), deployHandler);
      vertx.deployVerticle(new SVerticle("myServiceAddr2").responsePrefix("anytime2:"), deployHandler);
      vertx.deployVerticle(reg, deployHandler);
    });

    log.info("Sleeping");
    Thread.sleep(1000);
    Set<String> addrs = reg.getSubscriberAddresses();
    assertEquals(2, addrs.size());

    Future<Set<Object>> res = reg.query("callme1", "fromRegistry");
    assertEquals(2, res.get().size());
    assertTrue(res.get().contains("anytime:fromRegistry"));
    assertTrue(res.get().contains("anytime2:fromRegistry"));
  }

  @Test
  public void test3Server() throws InterruptedException {
    BVerticle reg = new BVerticle();
    createDeployLatch(4, (deployHandler) -> {
      vertx.deployVerticle(new SVerticle("myServiceAddr1"), deployHandler);
      vertx.deployVerticle(reg, deployHandler);
      vertx.deployVerticle(new SVerticle("myServiceAddr2"), deployHandler);
      vertx.deployVerticle(new SVerticle("myServiceAddr3"), deployHandler);
    });
    log.info("Sleeping");
    Thread.sleep(1000);
    Set<String> addrs = reg.getSubscriberAddresses();
    assertEquals(3, addrs.size());
  }
}
