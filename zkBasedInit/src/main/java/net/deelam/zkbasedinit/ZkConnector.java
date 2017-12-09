package net.deelam.zkbasedinit;

import java.nio.file.Paths;
import org.apache.commons.configuration2.Configuration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.WatchMode;
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

  public static String treeToString(CuratorFramework cf, String startupPath) {
    try {
      return ZKUtil
          .listSubTreeBFS(cf.getZookeeperClient().getZooKeeper(), Paths.get(startupPath).toString())
          .toString().replaceAll(",", "\n  ");
    } catch (Exception e) {
      return "Path does not exist: "+Paths.get(startupPath);
    }
  }

  public static void deletePath(CuratorFramework cf, String path) throws Exception {
    String appPrefixWithoutFinalSlash = Paths.get(path).toString();
    if (cf.checkExists().forPath(appPrefixWithoutFinalSlash) != null) {
      log.info("delete: {}", appPrefixWithoutFinalSlash);
      cf.delete().deletingChildrenIfNeeded().forPath(appPrefixWithoutFinalSlash);
    } else {
      log.warn("Path does not exist to delete: {}", appPrefixWithoutFinalSlash);
    }
  }

  public static void main(String[] args) throws Exception {
    String propFile = (args.length > 0) ? args[0] : "startup.props";
    Configuration config = ConfigReader.parseFile(propFile);
    log.info("{}\n------", ConfigReader.toStringConfig(config, config.getKeys()));
    
    String zkConnectionString=config.getString(ConstantsZk.ZOOKEEPER_CONNECT);
    String zkStartupPathHome=config.getString(ConstantsZk.ZOOKEEPER_STARTUPPATH);
    Injector injector = Guice.createInjector(new GModuleZooKeeper(zkConnectionString, zkStartupPathHome));
    CuratorFramework cf = injector.getInstance(CuratorFramework.class);
    String startupPath =
        injector.getInstance(Key.get(String.class, Names.named(ConstantsZk.ZOOKEEPER_STARTUPPATH)));
    log.info("Tree: {}", ZkConnector.treeToString(cf, Paths.get(startupPath).getParent().toString()));

  }
  
  public static void watchForNodeChange(CuratorFramework client, String watchedPath, ZNodeListener listener) {
    AsyncCuratorFramework.wrap(client).with(WatchMode.successOnly).watched().checkExists()
        .forPath(watchedPath).event() //
        .thenAcceptAsync( // acceptAsync so as not to block main EventThread
            evt -> {
              switch (evt.getType()) {
                case NodeCreated:
                  listener.nodeCreated(evt.getPath());
                  break;
                case NodeDataChanged:
                  listener.nodeDataChanged(evt.getPath());
                  break;
                case NodeDeleted:
                  listener.nodeDeleted(evt.getPath());
                  break;
                default:
                  listener.otherEvent(evt);
                  break;
              }
            });
  }

}
