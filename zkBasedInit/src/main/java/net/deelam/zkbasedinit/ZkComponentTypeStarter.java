package net.deelam.zkbasedinit;

import static net.deelam.zkbasedinit.ZkComponentStarter.*;
import static net.deelam.zkbasedinit.ZkComponentStarter.INIT_SUBPATH;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.function.Consumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class ZkComponentTypeStarter implements ZkComponentStarterI {

  private static final String COPIES_SUBPATH = "/copies";

  final CuratorFramework client;
  final String appPrefix;

  @Getter(lazy = true, value = AccessLevel.PRIVATE)
  private final AsyncCuratorFramework async = _async();

  private AsyncCuratorFramework _async() {
    return AsyncCuratorFramework.wrap(client);
  }

  @Setter
  private Consumer<ComponentI> componentStartedCallback;

  @Setter
  private Consumer<ComponentI> starterCompleteCallback;

  public void startWithCopyOf(String sourceSubpath, String componentPrefix, ComponentI comp)
      throws Exception {
    final String sourcePath = appPrefix + sourceSubpath;
    log.info("Starting component with copy of sourcePath={} to componentPrefix={}", sourcePath,
        componentPrefix);
    
    // wait for $sourcePath/init
    Stat initExists = client.checkExists().forPath(sourcePath + INIT_SUBPATH);
    if (initExists == null) {
      // path may not be created yet by ZkConfigPopulator
      // it must exist in order to watch for subpaths
      client.checkExists().creatingParentsIfNeeded().forPath(sourcePath);
      asyncWatchForInitPathEvent(sourcePath, componentPrefix, comp);
    } else {
      copyConfigAndStart(sourcePath, componentPrefix, comp);
    }
  }

  private void asyncWatchForInitPathEvent(String sourcePath, String componentPrefix, ComponentI comp) {
    log.info("watchForInitPathEvent: {}", sourcePath);
    getAsync().with(WatchMode.successOnly).watched().checkExists()
        // adds watch for path creation/deletion/change
        // https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_WatchSemantics
        .forPath(sourcePath + INIT_SUBPATH).event() // waits for event
        .thenAcceptAsync( // acceptAsync so as not to block main EventThread
            (evt) -> {
              EventType t = evt.getType();
              switch (t) {
                case NodeCreated:
                  copyConfigAndStart(sourcePath, componentPrefix, comp);
                  asyncWatchForInitPathEvent(sourcePath, componentPrefix, comp);
                  break;
                case NodeDeleted:
                  if (comp.isRunning())
                    log.warn("Component was not stopped before deleting ./init in sourcePath={}",
                        sourcePath);
                  break;
                default:
                  log.info("Ignoring event for ./init in path={}: {}", sourcePath, evt);
                  asyncWatchForInitPathEvent(sourcePath, componentPrefix, comp);
                  break;
              }
            });
  }

  /**
   * null until component's configuration is copied to an EPHEMERAL_SEQUENTIAL znode
   */
  @Getter
  private String componentId;

  private void copyConfigAndStart(String sourcePath, String componentPrefix, ComponentI comp) {
    // create ephemeral child znode $sourcePath/copies/$componentPrefix-001
    getAsync().create()
        .withOptions(EnumSet.of(CreateOption.createParentsIfNeeded),
            CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(sourcePath + COPIES_SUBPATH + "/" + componentPrefix + "-")
        .thenCompose(actualPath -> {
          componentId = Paths.get(actualPath).getFileName().toString();
          log.info("Created copy znode: {}", actualPath);

          // create ephemeral znode $appPrefix/$componentId, which should not already exist
          // if(client.checkExists().forPath(appPrefix + componentId)!=null)
          // throw new java.io.IOException("");
          return getAsync().create().forPath(appPrefix + componentId).thenAcceptAsync(newPath -> {
            log.info("Created new component at {}", newPath);
            try {
              copyConfAndInit(sourcePath, newPath);
            } catch (Exception e) {
              starterCompleteCallback.accept(comp);
              throw new RuntimeException(e);
            }
            try {
              ZkComponentStarter starter = new ZkComponentStarter(client, appPrefix);
              if (componentStartedCallback != null)
                starter.setComponentStartedCallback(componentStartedCallback);
              if (starterCompleteCallback != null)
                starter.setStarterCompleteCallback(starterCompleteCallback);
              starter.startWithId(componentId, comp);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
        });
  }

  private void copyConfAndInit(String sourcePath, String newPath) throws Exception {
    // copy sourcePath/conf to this new znode
    byte[] confData = client.getData().forPath(sourcePath + CONF_SUBPATH);
    client.create().forPath(newPath + CONF_SUBPATH, confData);
    
    byte[] confRefData = client.getData().forPath(sourcePath + CONFRESOLVED_SUBPATH);
    client.create().forPath(newPath + CONFRESOLVED_SUBPATH, confRefData);
    
    client.create().forPath(newPath + INIT_SUBPATH);
  }


}
