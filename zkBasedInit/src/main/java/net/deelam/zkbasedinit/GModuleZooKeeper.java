package net.deelam.zkbasedinit;

import java.nio.file.Paths;
import javax.inject.Named;
import org.apache.curator.framework.CuratorFramework;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class GModuleZooKeeper extends AbstractModule {

  public static final String ZOOKEEPER_CONNECT = Constants.ZOOKEEPER_CONNECT;

  final String zkConnectionString;
  final String zkStartupPathHome;

  @Override
  public void configure() {
    {
      // get from startup.prop file or System.properties
      String zookeeperConnectionString = System.getProperty(ZOOKEEPER_CONNECT);
      if (zookeeperConnectionString == null || zookeeperConnectionString.length()==0) {
        zookeeperConnectionString = zkConnectionString;
        if (zookeeperConnectionString == null)
          zookeeperConnectionString = "127.0.0.1:2181";
        log.info("System.setProperty: {}={}", ZOOKEEPER_CONNECT, zookeeperConnectionString);
        System.setProperty(ZOOKEEPER_CONNECT, zookeeperConnectionString);
      }
      
      // bind(String.class).annotatedWith(Names.named(ZOOKEEPER_CONNECT))
      // .toInstance(zookeeperConnectionString);
      
      bind(CuratorFramework.class)
        .toInstance(ZkConnector.connectToCluster(zookeeperConnectionString));
    }

    {
      String startupPathHome = System.getProperty(Constants.ZOOKEEPER_STARTUPPATH);
      if (startupPathHome == null || startupPathHome.length()==0) {
        startupPathHome = zkStartupPathHome;
        if (startupPathHome == null) 
          startupPathHome = "/test/app1/startup/";
      }
      log.info("System.setProperty: {}={}", Constants.ZOOKEEPER_STARTUPPATH, startupPathHome);
      System.setProperty(Constants.ZOOKEEPER_STARTUPPATH, startupPathHome);
      
      bind(String.class).annotatedWith(Names.named(Constants.ZOOKEEPER_STARTUPPATH))
          .toInstance(Paths.get(startupPathHome).toString()+"/");
    }
  }

  @Provides
  protected ZkConfigPopulator createZkConfigPopulator(CuratorFramework cf,
      @Named(Constants.ZOOKEEPER_STARTUPPATH) String startupPath) {
    return new ZkConfigPopulator(cf, startupPath);
  }
  
  @Provides
  protected ZkComponentStopper createZkComponentStopper(CuratorFramework cf,
      @Named(Constants.ZOOKEEPER_STARTUPPATH) String appPrefix) {
    return new ZkComponentStopper(cf, appPrefix);
  }
}
