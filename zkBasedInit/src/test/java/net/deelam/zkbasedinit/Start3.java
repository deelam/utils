package net.deelam.zkbasedinit;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;
import org.apache.curator.framework.CuratorFramework;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ConfigReader;
import net.deelam.zkbasedinit.ConstantsZk;
import net.deelam.zkbasedinit.GModuleZkComponentStarter;
import net.deelam.zkbasedinit.GModuleZooKeeper;
import net.deelam.zkbasedinit.ZkComponentStarter;
import net.deelam.zkbasedinit.ZkComponentStopper;
import net.deelam.zkbasedinit.ZkComponentTypeStarter;
import net.deelam.zkbasedinit.ZkConfigPopulator;
import net.deelam.zkbasedinit.ZkConnector;

@Slf4j
public class Start3 {


  public static void main(String[] args) throws Exception {
    String cIds = System.getProperty("componentIds", "jc1, jobStatus, amq, workerType, jobberA");
    List<String> compIdList =
        Arrays.stream(cIds.split(",")).map(c -> c.trim()).collect(Collectors.toList());
    log.info("componentIds to start: {}", compIdList);

    GModuleZkComponentStarter moduleZkComponentStarter = new GModuleZkComponentStarter(compIdList.size());
    Injector injector = Guice.createInjector( //
        new GModuleZooKeeper(null, "/test/start3/startup"), //
        moduleZkComponentStarter);
    CuratorFramework cf = injector.getInstance(CuratorFramework.class);

    String startupPath =
        injector.getInstance(Key.get(String.class, Names.named(ConstantsZk.ZOOKEEPER_STARTUPPATH)));

    ZkConfigPopulator cp = injector.getInstance(ZkConfigPopulator.class);
    try {
      //ZkConnector.deletePath(cf, "/test");
      //log.info("Tree before config: {}", ZkConnector.treeToString(cf, "/test"));
      populateConfiguration(cp);
      log.info("Tree after config: {}", ZkConnector.treeToString(cf, startupPath));
      Thread.sleep(1000);

      start(injector, compIdList);
      log.info("Awaiting components to start");
      moduleZkComponentStarter.getStartedLatch().await();

      log.info("Tree after all components started: {}", ZkConnector.treeToString(cf, startupPath));
      Thread.sleep(1000);
      stop(injector.getInstance(ZkComponentStopper.class));

      log.info("Awaiting components to finish");
      moduleZkComponentStarter.getCompletedLatch().await();
      log.info("Tree after stopped: {}", ZkConnector.treeToString(cf, startupPath));
    } finally {
      cp.cleanup();
    }
    log.info("Done");
  }


  private static void start(Injector injector, Collection<String> compIdSet) throws Exception {
    // In JVM that starts components given an compId and ComponentI subclass
    String compTypeName = "workerType";
    for (String compId : compIdSet) {
      FancyComponent fancyComp = null; // createComponent(compId);
      if (compId.equals(compTypeName)) {
        ZkComponentTypeStarter compStarter = injector.getInstance(ZkComponentTypeStarter.class);
        compStarter.startWithCopyOf(compId, "compPrefix", fancyComp);
      } else {
        ZkComponentStarter compStarter = injector.getInstance(ZkComponentStarter.class);
        compStarter.startWithId(compId, fancyComp);
      }
    }
  }

  private static void stop(ZkComponentStopper stopper) throws Exception, InterruptedException {
    // In JVM that stops system
    List<String> compIds = stopper.listRunningComponents();
    log.info("compIds={}", compIds);
    for (String compId : compIds) {
      stopper.stop(compId);
    }
    // stopper.stop(compMap.get(compTypeName).getComponentId());
  }

  private static void populateConfiguration(ZkConfigPopulator cp) throws Exception {
    // In JVM that populates components' configurations
    Configuration config = ConfigReader.parseFile("startup.props");
    log.info("{}\n------", ConfigReader.toStringConfig(config, config.getKeys()));

    Map<String, Configuration> subConfigMap = ConfigReader.extractSubconfigMap(config);
    log.info("componentIds in config: {}", subConfigMap.keySet());
    for (String compId : subConfigMap.keySet()) {
      cp.populateConfig(compId, subConfigMap.get(compId));
      if (compId.equals("amq")) {
        // simulate delayed amq start
      } else {
        cp.triggerInitializationWhenReady(compId, subConfigMap.get(compId));
      }
    }
    log.info("Config populated");

    Thread.sleep(1000);
    cp.triggerInitializationWhenReady("amq", subConfigMap.get("amq"));
  }
}
