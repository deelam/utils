package net.deelam.zkbasedinit;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.Supplier;
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

  final Supplier<String> zkConnectionStringS; // fallback values usually from startup.props
  final Supplier<String> zkStartupPathHomeS; // fallback values usually from startup.props

  public GModuleZooKeeper(String zkConnectionString, String zkStartupPathHome) {
    zkConnectionStringS=()->zkConnectionString;
    zkStartupPathHomeS=()->zkStartupPathHome;
  }
  
  // use this to avoid loading property file if not needed but assumes specific property keys
  public GModuleZooKeeper(Supplier<Properties> propertiesSupplier) {
    this(propertiesSupplier, ConstantsZk.ZOOKEEPER_CONNECT, ConstantsZk.ZOOKEEPER_STARTUPPATH);
  }  
  
  // use this to avoid loading property file if not needed
  public GModuleZooKeeper(Supplier<Properties> propertiesSupplier, String zkConnectionStringKey, String zkStartupPathHomeKey) {
    zkConnectionStringS = () -> propertiesSupplier.get().getProperty(zkConnectionStringKey);
    zkStartupPathHomeS = () -> propertiesSupplier.get().getProperty(zkStartupPathHomeKey);
  }  
  
  @Override
  public void configure() {
    {
      // get from startup.prop file or System.properties
      String zookeeperConnectionString = System.getProperty(ConstantsZk.ZOOKEEPER_CONNECT);
      if (zookeeperConnectionString == null || zookeeperConnectionString.length()==0) {
        zookeeperConnectionString = zkConnectionStringS.get();
        if (zookeeperConnectionString == null) {
          zookeeperConnectionString = "127.0.0.1:2181";
          log.warn("Using default zookeeperConnectionString={}", zookeeperConnectionString);
        }
        log.info("System.setProperty: {}={}", ConstantsZk.ZOOKEEPER_CONNECT, zookeeperConnectionString);
        System.setProperty(ConstantsZk.ZOOKEEPER_CONNECT, zookeeperConnectionString);
      }
      
      // bind(String.class).annotatedWith(Names.named(ZOOKEEPER_CONNECT))
      // .toInstance(zookeeperConnectionString);
      
      bind(CuratorFramework.class)
        .toInstance(ZkConnector.connectToCluster(zookeeperConnectionString));
    }

    {
      String startupPathHome = System.getProperty(ConstantsZk.ZOOKEEPER_STARTUPPATH);
      if (startupPathHome == null || startupPathHome.length()==0) {
        startupPathHome = zkStartupPathHomeS.get();
        if (startupPathHome == null) {
          startupPathHome = "/test/app3/startup/";
          log.warn("Using default startupPathHome={}", startupPathHome);
        }
        log.info("System.setProperty: {}={}", ConstantsZk.ZOOKEEPER_STARTUPPATH, startupPathHome);
        System.setProperty(ConstantsZk.ZOOKEEPER_STARTUPPATH, startupPathHome);
      }
      
      // used by GModuleZkComponentStarter
      bind(String.class).annotatedWith(Names.named(ConstantsZk.ZOOKEEPER_STARTUPPATH))
          .toInstance(Paths.get(startupPathHome).toString()+"/");
    }
  }

  @Provides
  protected ZkConfigPopulator createZkConfigPopulator(CuratorFramework cf,
      @Named(ConstantsZk.ZOOKEEPER_STARTUPPATH) String startupPath) {
    return new ZkConfigPopulator(cf, startupPath);
  }
  
  @Provides
  protected ZkComponentStopper createZkComponentStopper(CuratorFramework cf,
      @Named(ConstantsZk.ZOOKEEPER_STARTUPPATH) String appPrefix) {
    return new ZkComponentStopper(cf, appPrefix);
  }
}
