package net.deelam.activemq.rpc;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.activemq.broker.BrokerService;
import org.junit.*;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.MQService;
import net.deelam.activemq.rpc.ActiveMqRpcClient;
import net.deelam.activemq.rpc.ActiveMqRpcServer;

@Slf4j
public class ActiveMqRpcClientTest {

  static final String brokerURL = "tcp://localhost:56789";
  private String serverAddr="hdfsSvc123";
  private BrokerService broker;

  @Before
  public void before() throws Exception {
    broker=MQService.createBrokerService("amqClientTestBroker", brokerURL);
    Thread.sleep(1000);
  }

  @After
  public void after() throws Exception {
    broker.stop();
    Thread.sleep(1000);
  }
  
  @Test
  public void testServerFirst() throws IOException, InterruptedException, ExecutionException {
    startServer();
    Thread.sleep(1000);

    ActiveMqRpcClient client = startClient();
    HdfsSvcI hdfs = client.createRpcClient(serverAddr, HdfsSvcI.class, true);
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
    
    ActiveMqRpcClient client = startClient();
    HdfsSvcI hdfs = client.createRpcClient(serverAddr, HdfsSvcI.class, true); // blocks
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
    
    ActiveMqRpcClient client = startClient();
    HdfsSvcI hdfs = client.createRpcClient(serverAddr, HdfsSvcI.class, true); // blocks
    Boolean boolean1 = hdfs.exists("src").get();
    assertTrue(boolean1);
    log.info("exists=" + boolean1); //blocks
    
    //client.invalidateAndFindNewServer(HdfsInterface.class);
    Boolean boolean2 = hdfs.exists("src").get();
    assertTrue(boolean2);
    log.info("exists=" + boolean2); //blocks
  }


  private ActiveMqRpcClient startClient() {
    return new ActiveMqRpcClient(null, brokerURL); //.start();
  }

  private void startServer() {
    new ActiveMqRpcServer(brokerURL).start(serverAddr, new RemoteSvc());
  }

}
