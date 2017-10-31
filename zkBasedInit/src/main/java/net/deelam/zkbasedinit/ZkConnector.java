package net.deelam.zkbasedinit;

import java.nio.file.Paths;
import org.apache.commons.configuration2.Configuration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZKUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ZkConnector {
  private ZkConnector() {}

  // only need one CuratorFramework object for each ZooKeeper cluster you are connecting to
  public static CuratorFramework connectToCluster(String zookeeperConnectionString) {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client =
        CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
    client.start();
    return client;
  }

  public static String treeToString(CuratorFramework cf, String startupPath) throws Exception {
    return ZKUtil
        .listSubTreeBFS(cf.getZookeeperClient().getZooKeeper(), Paths.get(startupPath).toString())
        .toString().replaceAll(",", "\n  ");
  }

  static void deletePath(CuratorFramework cf, String path) throws Exception {
    String appPrefixWithoutFinalSlash = Paths.get(path).toString();
    if (cf.checkExists().forPath(appPrefixWithoutFinalSlash) != null) {
      log.info("delete: {}", appPrefixWithoutFinalSlash);
      cf.delete().deletingChildrenIfNeeded().forPath(appPrefixWithoutFinalSlash);
    } else {
      log.warn("Path does not exist to delete: {}", appPrefixWithoutFinalSlash);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration config = ConfigReader.parseFile("startup.props");
    log.info("{}\n------", ConfigReader.toStringConfig(config, config.getKeys()));
    
    Injector injector = Guice.createInjector(new GModuleZooKeeper(config));
    CuratorFramework cf = injector.getInstance(CuratorFramework.class);
    String startupPath =
        injector.getInstance(Key.get(String.class, Names.named(Constants.ZOOKEEPER_STARTUPPATH)));
    log.info("Tree: {}", ZkConnector.treeToString(cf, Paths.get(startupPath).getParent().toString()));

  }
}
