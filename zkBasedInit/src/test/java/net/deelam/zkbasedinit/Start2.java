package net.deelam.zkbasedinit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.configuration2.Configuration;
import org.apache.curator.framework.CuratorFramework;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ConfigReader;
import net.deelam.zkbasedinit.ZkComponentStarter;
import net.deelam.zkbasedinit.ZkComponentStarterI;
import net.deelam.zkbasedinit.ZkComponentStopper;
import net.deelam.zkbasedinit.ZkComponentTypeStarter;
import net.deelam.zkbasedinit.ZkConfigPopulator;
import net.deelam.zkbasedinit.ZkConnector;

@Slf4j
public class Start2 {
  public static void main(String[] args) throws Exception {
    new Start2().go();
  }


  void go() throws Exception {
    Configuration config = ConfigReader.parseFile("startup.props");
    log.info("{}\n------", ConfigReader.toStringConfig(config, config.getKeys()));

    // get from startup.prop file or System.properties
    String zookeeperConnectionString = config.getString("ZOOKEEPER.CONNECT");
    if (zookeeperConnectionString == null) {
      final String zkIP = System.getProperty("ZOOKEEPER.IP", "127.0.0.1");
      final String zkPort = System.getProperty("ZOOKEEPER.PORT", "2181");
      zookeeperConnectionString = System.getProperty("ZOOKEEPER.CONNECT", zkIP + ":" + zkPort);
    }

    String appPrefix = config.getString("ZOOKEEPER.STARTUPPATH");
    if (appPrefix == null)
      appPrefix = System.getProperty("ZOOKEEPER.STARTUPPATH", "/test/app1/startup/");

    // assumes Zookeeper already started
    CuratorFramework cf = ZkConnector.connectToCluster(zookeeperConnectionString);
    ZkConfigPopulator cp = new ZkConfigPopulator(cf, appPrefix);

    Map<String, Configuration> subConfigMap = ConfigReader.extractSubconfigMap(config);
    log.info("componentIds={}", subConfigMap.keySet());
    try {
      CountDownLatch startedLatch = new CountDownLatch(subConfigMap.keySet().size());
      CountDownLatch stoppedLatch = new CountDownLatch((int) startedLatch.getCount());

      Map<String, ZkComponentStarterI> compMap = new HashMap<>();
      String compTypeName = "jobberA";
      for (String compId : subConfigMap.keySet()) {
        ZkComponentStarterI comp;
        if (compId.equals(compTypeName))
          comp = startComponentOfType(cf, appPrefix, compId, startedLatch, stoppedLatch);
        else
          comp = startComponent(cf, appPrefix, compId, startedLatch, stoppedLatch);
        compMap.put(compId, comp);
      }
      Thread.sleep(1000);

      { // In JVM that populates components' configurations 
        for (String compId : subConfigMap.keySet()) {
          cp.populateConfig(compId, subConfigMap.get(compId));
          if (compId.equals("amq")) {
            //simulate delayed amq start
          } else {
            cp.triggerInitializationWhenReady(compId, subConfigMap.get(compId));
          }
        }
        log.info("Config populated");

        Thread.sleep(1000);
        cp.triggerInitializationWhenReady("amq", subConfigMap.get("amq"));
      }

      log.info("Awaiting components to start");
      startedLatch.await();

      Thread.sleep(1000);
      
      { // In JVM that stops system
        ZkComponentStopper stopper = new ZkComponentStopper(cf, appPrefix);
        List<String> compIds=stopper.listRunningComponents();
        log.info("compIds={}", compIds);
        for (String compId : compIds) {
          stopper.stop(compId);
        }
        //stopper.stop(compMap.get(compTypeName).getComponentId());
      }

      log.info("Awaiting starter to finish");
      stoppedLatch.await();

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cp.cleanup();
    }
    log.info("Done");
  }

  private static ZkComponentStarterI startComponent(CuratorFramework cf, String appPrefix,
      String componentId, CountDownLatch startedLatch, CountDownLatch completeLatch)
      throws Exception {
    ZkComponentStarter compStarter = new ZkComponentStarter(cf, appPrefix);
    FancyComponent fancyComp = createComponent(startedLatch, completeLatch, compStarter);
    compStarter.startWithId(componentId, fancyComp);
    return compStarter;
  }

  private static ZkComponentStarterI startComponentOfType(CuratorFramework cf, String appPrefix,
      String componentType, CountDownLatch startedLatch, CountDownLatch completeLatch)
      throws Exception {
    ZkComponentTypeStarter compStarter = new ZkComponentTypeStarter(cf, appPrefix);
    FancyComponent fancyComp = createComponent(startedLatch, completeLatch, compStarter);
    compStarter.startWithCopyOf(componentType, "compPrefix", fancyComp);
    return compStarter;
  }

  public static FancyComponent createComponent(CountDownLatch startedLatch,
      CountDownLatch completeLatch, ZkComponentStarterI compStarter) {
    FancyComponent fancyComp = new FancyComponent();
    compStarter.setComponentStartedCallback(c -> startedLatch.countDown());
    compStarter.setStarterCompleteCallback(c -> completeLatch.countDown());
    return fancyComp;
  }
}
