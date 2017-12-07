package net.deelam.vertx.rpc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.rpc.RpcVerticleClient;
import net.deelam.vertx.rpc.RpcVerticleServer;

@Slf4j
public class RpcVerticleClientTest {

  Vertx vertx;
  private String serversBroadcastAddr = "serverBroadcastInbox";

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @Test
  public void testServerFirst() throws IOException, InterruptedException, ExecutionException {
    startServer();
    Thread.sleep(1000);

    RpcVerticleClient client = startClient();
    SvcInterface hdfs = client.createRpcClient(SvcInterface.class).get();
    log.info("exists=" + hdfs.exists("src").get());
  }
  
  @Test
  public void testClientFirst() throws InterruptedException, ExecutionException, IOException {
    new Thread(() -> {
      try {
        Thread.sleep(6000);
      } catch (Exception e) {
        e.printStackTrace();
      }
      startServer();
    }).start();
    
    RpcVerticleClient client = startClient();
    SvcInterface hdfs = client.createRpcClient(SvcInterface.class).get(); // blocks
    Boolean boolean1 = hdfs.exists("src").get();
    assertTrue(boolean1);
    log.info("exists=" + boolean1); //blocks
  }

  @Test
  public void testClientFirstThenInvalidate() throws InterruptedException, ExecutionException, IOException {
    new Thread(() -> {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        e.printStackTrace();
      }
      startServer();
    }).start();
    
    RpcVerticleClient client = startClient();
    SvcInterface hdfs = client.createRpcClient(SvcInterface.class).get(); // blocks
    Boolean boolean1 = hdfs.exists("src").get();
    assertTrue(boolean1);
    log.info("exists=" + boolean1); //blocks
    
    client.invalidateAndFindNewServer(SvcInterface.class);
    Boolean boolean2 = hdfs.exists("src").get();
    assertTrue(boolean2);
    log.info("exists=" + boolean2); //blocks
  }


  private RpcVerticleClient startClient() {
    return new RpcVerticleClient(vertx, serversBroadcastAddr).start();
  }

  private void startServer() {
    new RpcVerticleServer(vertx, serversBroadcastAddr)
        .start("myServerAddr", new RemoteSvc(), true);
  }

}
