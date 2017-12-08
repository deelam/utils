package net.deelam.coordworkers;

import java.nio.file.Paths;
import java.util.Properties;
import org.apache.commons.configuration2.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ConfigReader;
import net.deelam.zkbasedinit.GModuleZooKeeper;
import net.deelam.zkbasedinit.ZNodeListener;
import net.deelam.zkbasedinit.ZkComponentStarter;
import net.deelam.zkbasedinit.ZkComponentStopper;
import net.deelam.zkbasedinit.ZkConfigPopulator;
import net.deelam.zkbasedinit.ZkConnector;

/**
 * <pre>
 * Start ZK and populate
 * Start components:
 * - ActiveMQ service: shares connectionUrl
 * - Submitter: sends to submitJob queue; listens to jobStatus topic
 * - Jobber: shares submitJob queue and availableJobs topic names; listens to jobStatus topic
 * - Worker: listens on workers and availableJobs topics; publishes to jobStatus topic
 *
 * </pre>
 */
@Slf4j
public class RunSystem2 {

  public static void main(String[] args) throws Exception {
    try {
      ZkComponentStopper.main(args);
      Thread.sleep(1000);
    } catch (Exception e) {
      log.info("Tree doesn't exist yet");
    }

    System.setProperty("componentIds", "amq, submitterA, jobberB, workerType");
    // in JVM 1, Start ZK and populate
    new Thread(() -> {
      try {
        ZkConfigPopulator.main(new String[] {"configs.props"});
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "addConfigToZK").start();

    Thread.sleep(4000); // fix for possible race condition with ZkComponentStarter?
    
    // in JVM 2, Start components
    new Thread(() -> {
      try {
        ZkComponentStarter.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "startComponents").start();

    //
      new Thread(() -> {
        try {
          Configuration config = ConfigReader.parseFile("startup.props");
          String zookeeperConnectionString=config.getString(GModuleZooKeeper.ZOOKEEPER_CONNECT, "127.0.0.1:2181");
          String prefix=config.getString("ZOOKEEPER.STARTUPPATH", "/test/coordworkers/startup/");
          CuratorFramework client = ZkConnector.connectToCluster(zookeeperConnectionString);
          String path=prefix+"workerType/copies";
          try(PathChildrenCache cache=new PathChildrenCache(client, path, true)){
            cache.start(StartMode.POST_INITIALIZED_EVENT);
            cache.getListenable().addListener((client2, event)->{
              byte[] byteArr = (event.getData()==null)?null:event.getData().getData();
              log.info("CACHE EVENT: type={}, data={}", event.getType(), (byteArr==null)?null:new String(byteArr));
              log.info("CACHE EVENT: initialData={}", event.getInitialData());
              switch(event.getType()) {
                case CHILD_ADDED:
                  break;
                case CHILD_REMOVED:
                  break;
                case CHILD_UPDATED:
                  printCache(client, event.getData());
                  break;
              }
            });
            log.info("CACHE: currData1={}",cache.getCurrentData());
            Thread.sleep(10000);
            cache.getCurrentData().forEach(cd->{
              printCache(client, cd);
            });
          };
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, "watchForCopiesOfType").start();
    
    Thread.sleep(30000);

    // in JVM 3
    new Thread(() -> {
      try {
        ZkComponentStopper.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "stopComponents").start();

    log.info("Done");
  }

  public static void printCache(CuratorFramework client, ChildData cd) {
    byte[] byteArr = cd.getData();
    log.info("CACHE currData2: path={}, data={}", cd.getPath(), new String(byteArr));
    String newPath=new String(byteArr);
    String componentId = Paths.get(newPath).getFileName().toString();
    
    String watchedPath = newPath + ZkComponentStarter.STARTED_SUBPATH;
    if(!true) {
      try(NodeCache startedPath=new NodeCache(client, watchedPath)){
        startedPath.start();
        startedPath.getListenable().addListener(new NodeCacheListener() {
          @Override
          public void nodeChanged() throws Exception {
            // TODO Auto-generated method stub
            
          }
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      ZkConnector.watchForNodeChange(client, watchedPath, new ZNodeListener() {
        @Override
        public void nodeCreated(String path) {
          try {
            Properties compConfig = ZkComponentStarter.getConfig(client, newPath, componentId);
            log.info("CACHE: compConfig={}", compConfig);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
  }


}
