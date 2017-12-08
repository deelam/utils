package net.deelam.zkbasedinit;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.WatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Starts the specified component and watches the INIT_SUBPATH znode for events. If znode changes,
 * then tries to reinit the component with the latest config at CONF_SUBPATH. If znode is deleted,
 * then stops component, deletes STARTED_SUBPATH, and calls starterCompleteCallback.
 *
 * If component stops ungracefully (i.e., when ephemeral STARTED_SUBPATH znode is deleted but
 * INIT_SUBPATH znode still exists), then this class will try to stop the component if needed, then
 * call the starterCompleteCallback. With that said, the STARTED_SUBPATH znode will not be
 * automatically deleted by zookeeper until Zookeeper client session is closed. For the
 * ungraceful-stop detection to be useful, the component should create the session.
 * 
 */
@RequiredArgsConstructor
@Slf4j
public class ZkComponentStarter implements ZkComponentStarterI {

  static final String CONF_SUBPATH = ZkConfigPopulator.CONF_SUBPATH;
  static final String CONFRESOLVED_SUBPATH = ZkConfigPopulator.CONFRESOLVED_SUBPATH;

  /**
   * znode creation signifies CONF_SUBPATH is ready; znode deletion triggers component to stop
   */
  static final String INIT_SUBPATH = "/init";

  /**
   * EPHEMERAL znode used to detect if component is still up
   */
  static final String STARTED_SUBPATH = "/started";

  ///

  final CuratorFramework client;
  final String appPrefix;

  @Getter(lazy = true, value = AccessLevel.PRIVATE)
  private final AsyncCuratorFramework async = _async();

  @SuppressWarnings("squid:S00100")
  private AsyncCuratorFramework _async() {
    return AsyncCuratorFramework.wrap(client);
  }

  ///

  String path;
  @Getter
  String componentId;
  ComponentI component;

  private void init(String zkPath, String componentId, ComponentI comp) {
    path = zkPath + componentId;
    this.componentId = componentId;
    component = comp;
  }

  /**
   * If configuration is ready, starts component synchronously. Otherwise, asynchronously waits for
   * configuration to be ready before starting component.
   * 
   * @param componentId
   * @param comp
   * @throws Exception
   */
  public void startWithId(String componentId, ComponentI comp) throws Exception {
    init(appPrefix, componentId, comp);
    log.info("Starting: path={}", path);
    Stat initExists = client.checkExists().forPath(path + INIT_SUBPATH);
    if (initExists == null) {
      // path may not be created yet by ZkConfigPopulator
      // it must exist in order to watch for subpaths
      client.checkExists().creatingParentsIfNeeded().forPath(path);
      asyncWatchForInitPathEvent();
    } else {
      startAndWatchOtherwiseQuit();
    }
  }

  @Setter
  private Consumer<ComponentI> componentStartedCallback =
      comp -> log.info("Component started: {}", comp);

  /**
   * Starter is complete when component could not be started or when component stopped. At this
   * point, the Starter stops watching znodes.
   */
  @Setter
  private Consumer<ComponentI> starterCompleteCallback = comp -> log.info("Starter done: {}", comp);

  private boolean initNodeDeleted;

  private void asyncWatchForInitPathEvent() {
    log.info("asyncWatchForInitPathEvent: {}", path);
    getAsync().with(WatchMode.successOnly).watched().checkExists()
        // adds watch for path creation/deletion/change
        // https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_WatchSemantics
        .forPath(path + INIT_SUBPATH).event() // waits for event
        .thenAcceptAsync( // acceptAsync so as not to block main EventThread
            evt -> {
              switch (evt.getType()) {
                case NodeCreated:
                  startAndWatchOtherwiseQuit();
                  break;
                case NodeDataChanged:
                  try {
                    if (component.reinit(getConfig(path))) {
                      setSharedValues();
                      getAsync().setData().forPath(path + STARTED_SUBPATH);
                    }
                  } catch (Exception e) {
                    log.error("Could not reinit component with path=" + path, e);
                  }
                  asyncWatchForInitPathEvent();
                  break;
                case NodeDeleted:
                  initNodeDeleted = true;
                  if (component.isRunning()) {
                    stopComponent();
                  } else {
                    log.warn("INIT_SUBPATH deleted but component is not running: {}", path);
                    starterCompleteCallback.accept(component);
                  }
                  break;
                default:
                  log.info("Ignoring event for ./init in path={}: {}", path, evt);
                  asyncWatchForInitPathEvent();
                  break;
              }
            });
  }

  private void startAndWatchOtherwiseQuit() {
    if (startComponent())
      asyncWatchForInitPathEvent();
    else
      starterCompleteCallback.accept(component);
  }

  public boolean startComponent() {
    try {
      Stat alreadyStarted = client.checkExists().forPath(path + STARTED_SUBPATH);
      if (alreadyStarted == null) {
        Properties props = getConfig(path);
        if (component == null)
          component = instantiateComponent(props);
        component.start(props);
        setSharedValues();
        client.create().withMode(CreateMode.EPHEMERAL).forPath(path + STARTED_SUBPATH);
        componentStartedCallback.accept(component);
        asyncWatchForStartPathEvent();
        return true;
      } else {
        log.warn("Component already exists with path={}", path + STARTED_SUBPATH);
        return false;
      }
    } catch (Exception e) {
      log.error("Could not start component with path=" + path, e);
      return false;
    }
  }

  private void setSharedValues() {
    component.getSharedValuesMap().forEach((subpath, val) -> {
      String fullpath = Paths.get(path, subpath).toString();
      log.info("Setting shared value: {}={}", fullpath, val);
      byte[] data = null;
      if (val instanceof byte[])
        data = (byte[]) val;
      else
        try {
          data = SerializeUtils.serialize(val);
        } catch (IOException e) {
          log.error("When serializing shared values", e);
        }
      if (data != null)
        try {
          client.create().withMode(CreateMode.EPHEMERAL).forPath(fullpath, data);
        } catch (Exception e) {
          log.error("When creating node for shared value at: " + fullpath, e);
        }
    });
  }

  private ComponentI instantiateComponent(Properties props)
      throws NoSuchMethodException, ClassNotFoundException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    String className = props.getProperty("classname");
    if (className == null) {
      throw new IllegalArgumentException(
          "Since component=null, must provide 'classname' property for componentId=" + componentId);
    } else {
      log.info("Creating instance of component: {}", className);
      Class<?> clazz = Class.forName(className);
      Constructor<?> ctor = clazz.getConstructor();
      return (ComponentI) ctor.newInstance();
    }
  }

  private void stopComponent() {
    component.stop();
    try {
      client.delete().forPath(path + STARTED_SUBPATH);
    } catch (Exception e) {
      log.error("Could not delete path=" + path, e);
    }
    starterCompleteCallback.accept(component);
  }

  private void asyncWatchForStartPathEvent() {
    log.info("asyncWatchForStartPathEvent: {}", path);
    getAsync().with(WatchMode.successOnly).watched().checkExists().forPath(path + STARTED_SUBPATH)
        .event().thenAcceptAsync(evt -> {
          switch (evt.getType()) {
            case NodeDeleted:
              if (!initNodeDeleted) {
                log.warn("Component stopped ungracefully, INIT_SUBPATH still exists: {}",
                    path + INIT_SUBPATH);
                if (component.isRunning()) {
                  stopComponent();
                } else {
                  starterCompleteCallback.accept(component);
                }
              }
              break;
            default:
              log.info("Ignoring event for ./init in path={}: {}", path, evt);
              asyncWatchForStartPathEvent();
              break;
          }
        });
  }

  private Properties getConfig(String pathPrefix) throws Exception {
    byte[] confData = client.getData().forPath(pathPrefix + CONF_SUBPATH);
    Properties configMap = SerializeUtils.deserializeConfigurationAsProperties(confData);
    log.info("Got config: {}: {}", pathPrefix, configMap);
    configMap.put(ComponentI.ZK_PATH, path);
    configMap.put(ComponentI.COMPONENT_ID, componentId);
    
    // add values for refPaths
    if(client.checkExists().forPath(pathPrefix + CONFRESOLVED_SUBPATH)!=null) {
      byte[] refPathData = client.getData().forPath(pathPrefix + CONFRESOLVED_SUBPATH);
      Properties configRefMap = (Properties) SerializeUtils.deserialize(refPathData);
      configMap.putAll(configRefMap);
    }
    return configMap;
  }

  public static void main(String[] args) throws Exception {
    String propFile = (args.length > 0) ? args[0] : "startup.props";
    Configuration config = ConfigReader.parseFile(propFile);
    log.info("{}\n------", ConfigReader.toStringConfig(config, config.getKeys()));

    String cIds = System.getProperty("componentIds",
        config.getString("componentIds", "jc1, jobStatus, amq, workerType, jobberA"));
    List<String> compIdList =
        Arrays.stream(cIds.split(",")).map(String::trim).collect(Collectors.toList());

    go(config, compIdList);
  }

  public static void go(Configuration config, List<String> compIdList)
      throws Exception {
    log.info("componentIds to start: {}", compIdList);
    GModuleZkComponentStarter moduleZkComponentStarter =
        new GModuleZkComponentStarter(compIdList.size());
    Injector injector = Guice.createInjector( //
        new GModuleZooKeeper(config), //
        moduleZkComponentStarter);

    CuratorFramework cf = injector.getInstance(CuratorFramework.class);
    String startupPath =
        injector.getInstance(Key.get(String.class, Names.named(Constants.ZOOKEEPER_STARTUPPATH)));

    // starts components given an compId and ComponentI subclass
    for (String compId : compIdList) {
      startComponent(injector, compId);
      log.info("Tree after starting {}: {}", compId, ZkConnector.treeToString(cf, startupPath));
    }

    Thread.sleep(1000);
    log.info("Awaiting components to start: {}",
        moduleZkComponentStarter.getStartedLatch().getCount());
    moduleZkComponentStarter.getStartedLatch().await();

    log.info("All components started: {}", compIdList);
    log.info("Tree after all components started: {}", ZkConnector.treeToString(cf, startupPath));
    Thread.sleep(1000);
    log.info("Awaiting components to end: {}",
        moduleZkComponentStarter.getCompletedLatch().getCount());
    moduleZkComponentStarter.getCompletedLatch().await();

    log.info("Tree after components stopped: {}", ZkConnector.treeToString(cf, startupPath));
  }

  public static void startComponent(Injector injector, String compId) throws Exception {
    ComponentI aComp = null;
    if (compId.endsWith("Type")) {
      ZkComponentTypeStarter compStarter = injector.getInstance(ZkComponentTypeStarter.class);
      compStarter.startWithCopyOf(compId, compId.substring(0, compId.length() - 4), aComp);
    } else {
      ZkComponentStarter compStarter = injector.getInstance(ZkComponentStarter.class);
      compStarter.startWithId(compId, aComp);
    }
  }
}
