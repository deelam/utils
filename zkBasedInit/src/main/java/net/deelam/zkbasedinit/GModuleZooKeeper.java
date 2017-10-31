package net.deelam.zkbasedinit;

import java.nio.file.Paths;
import javax.inject.Named;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.curator.framework.CuratorFramework;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class GModuleZooKeeper extends AbstractModule {

  private static final String ZOOKEEPER_CONNECT = "ZOOKEEPER.CONNECT";

  final Configuration configuration;

  @Override
  public void configure() {
    ImmutableConfiguration config = configuration == null ? new BaseConfiguration() : configuration;

    {
      // get from startup.prop file or System.properties
      String zookeeperConnectionString = System.getProperty(ZOOKEEPER_CONNECT);
      if (zookeeperConnectionString == null) {
//        final String zkIP = System.getProperty("ZOOKEEPER.IP", "127.0.0.1");
//        final String zkPort = System.getProperty("ZOOKEEPER.PORT", "2181");
        zookeeperConnectionString = config.getString(ZOOKEEPER_CONNECT, "127.0.0.1:2181");
      }
      // bind(String.class).annotatedWith(Names.named(ZOOKEEPER_CONNECT))
      // .toInstance(zookeeperConnectionString);
      bind(CuratorFramework.class)
          .toInstance(ZkConnector.connectToCluster(zookeeperConnectionString));
    }

    {
      String startupPathHome = System.getProperty(Constants.ZOOKEEPER_STARTUPPATH);
      if (startupPathHome == null)
        startupPathHome = config.getString(Constants.ZOOKEEPER_STARTUPPATH, "/test/app1/startup/");
      bind(String.class).annotatedWith(Names.named(Constants.ZOOKEEPER_STARTUPPATH))
          .toInstance(Paths.get(startupPathHome).toString()+"/");
    }
  }

//  @Provides
//  protected CuratorFramework createCuratorFramework(
//      @Named(ZOOKEEPER_CONNECT) String zookeeperConnectionString) {
//    // assumes Zookeeper already started
//    return ZkConnector.connectToCluster(zookeeperConnectionString);
//  }

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
