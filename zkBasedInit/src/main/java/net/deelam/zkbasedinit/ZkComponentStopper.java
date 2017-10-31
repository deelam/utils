package net.deelam.zkbasedinit;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import com.google.inject.Guice;
import com.google.inject.Injector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 *
 */
@RequiredArgsConstructor
@Slf4j
public class ZkComponentStopper {

  static final String INIT_SUBPATH = ZkComponentStarter.INIT_SUBPATH;

  final CuratorFramework client;
  final String appPrefix;

  String path;
  String componentId;

  private void init(String zkPath, String componentId) {
    path = zkPath + componentId;
    this.componentId = componentId;
  }

  public boolean stop(String componentId) throws Exception {
    init(appPrefix, componentId);
    log.info("Stopping: path={}", path);
    Stat initExists = client.checkExists().forPath(path + INIT_SUBPATH);
    if (initExists == null) {
      log.warn("Path doesn't exist: {}", path + INIT_SUBPATH);
      return false;
    } else {
      client.delete().forPath(path + INIT_SUBPATH);
      return true;
    }
  }

  public List<String> listRunningComponents() {
    try {
      List<String> childs = client.getChildren().forPath(Paths.get(appPrefix).toString());
      log.info("subdirs={}", childs);
      return childs.stream().map(child -> {
        try {
          Stat isRunning =
              client.checkExists().forPath(appPrefix + child + ZkComponentStarter.STARTED_SUBPATH);
          if (isRunning != null)
            return child;
        } catch (Exception e) {
          log.warn("When checking if component is running", e);
        }
        return null;
      }).filter(Objects::nonNull).collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("When getting subdirs of path", e);
      return Collections.emptyList();
    }
  }
  
  public void cleanup() throws Exception {
    log.info("cleanup: {}", appPrefix);
    ZkConnector.deletePath(client, appPrefix);
  }

  public static void main(String[] args) throws Exception {
    Configuration config = ConfigReader.parseFile("startup.props");
    log.info("{}\n------", ConfigReader.toStringConfig(config, config.getKeys()));

    Injector injector = Guice.createInjector(new GModuleZooKeeper(config));
    ZkComponentStopper stopper = injector.getInstance(ZkComponentStopper.class);
    log.info("Tree before stopping: {}", ZkConnector.treeToString(stopper.client, stopper.appPrefix));

    List<String> compIds = stopper.listRunningComponents();
    log.info("compIds to stop: {}", compIds);
    compIds.forEach(compId -> {
      try {
        stopper.stop(compId);
      } catch (Exception e) {
        log.error("When stopping compId="+compId, e);
      }
    });
    
    Thread.sleep(2000); // allow time for modifications to take effect
    log.info("Tree after stopping: {}", ZkConnector.treeToString(stopper.client, stopper.appPrefix));

    boolean cleanUp=true; //args.length>0 && "clean".equals(args[0]);
    if (cleanUp) {
      stopper.cleanup();
    }
  }

}
