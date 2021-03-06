package net.deelam.zkbasedinit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.WatchMode;
import com.google.inject.Guice;
import com.google.inject.Injector;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Accessors(chain = true)
@Slf4j
public class ZkConfigPopulator {
  static final String CONF_SUBPATH = "/conf";
  static final String CONFRESOLVED_SUBPATH = "/conf/resolved";

  @Getter
  final CuratorFramework client;
  @Getter
  final String appPrefix;

  @Getter(lazy = true, value = AccessLevel.PRIVATE)
  private final AsyncCuratorFramework async = _async();

  @SuppressWarnings("squid:S00100")
  private AsyncCuratorFramework _async() {
    return AsyncCuratorFramework.wrap(client);
  }
  
  public void close() {
    if(completeLatch!=null)
      if(completeLatch.getCount()>0)
        log.error("Configuration not fully populated!  Was still waiting for required components to start.");
    log.info("Remember to close client connection to Zookeeper: {}", client);
  }

  void populateConfig(String componentId, Configuration configuration) throws Exception {
    String confPath = appPrefix + componentId + CONF_SUBPATH;
    log.info("ZKCONFIG: populateConfig: {}", confPath);

    if (client.checkExists().forPath(confPath) != null) {
      log.warn("When populating config for {}", componentId, new IOException("Path already exists: " + confPath));
      // OPTIONALLY: client.delete().deletingChildrenIfNeeded().forPath(confPath)
    }

    byte[] data = SerializeUtils.serialize(configuration);
    client.create().creatingParentsIfNeeded().forPath(confPath, data);
  }

  void triggerInitializationWhenReady(String componentId, Configuration configuration) {
    log.info("ZKCONFIG: triggerInitializationWhenReady: {}", componentId);
    List<String> remainingReqPaths = getRemainingReqPathsFor(configuration);
    if (remainingReqPaths.isEmpty()) {
      triggerComponentInitialization(componentId);
    } else {
      watchForRequiredPaths(remainingReqPaths, c -> {
        if (getRemainingReqPathsFor(configuration).isEmpty()) {
          log.info("ZKCONFIG: requiredComponents satisfied for {}", componentId);
          triggerComponentInitialization(componentId);
        }
      });
    }
  }

  private List<String> getRemainingReqPathsFor(Configuration configuration) {
    List<String> reqComps = getPaths(configuration.getList(String.class, "requiredComponents"),
        cId -> cId + ZkComponentStarter.STARTED_SUBPATH);

    List<String> reqPaths =
        getPaths(configuration.getList(String.class, "requiredPaths"), Function.identity());

    List<String> refPaths = getRefPaths(configuration);

    List<String> paths = new ArrayList<>();
    paths.addAll(reqComps);
    paths.addAll(reqPaths);
    paths.addAll(refPaths);
    return paths;
  }

  private Properties refPathValues = new Properties();
  private static final String REF_SUFFIX = ".ref";

  private List<String> getRefPaths(Configuration configuration) {
    List<String> refPaths = new ArrayList<>();
    configuration.getKeys().forEachRemaining(k -> {
      if (k.endsWith(REF_SUFFIX)) {
        String relativePath = configuration.getString(k);
        String watchPath = appPrefix + relativePath;
        if (!refPathValues.containsKey(relativePath) || refPathValues.get(relativePath) == null) {
          byte[] value = getDataIfExists(watchPath);
          if (value == null)
            refPaths.add(watchPath);
          else
            try {
              refPathValues.put(relativePath, SerializeUtils.deserialize(value));
            } catch (ClassNotFoundException | IOException e) {
              log.warn("When deserializing referenced path value",e);
              exceptionHandler.accept(e);
              refPathValues.put(relativePath, value);
            }
        }
      }
    });
    return refPaths;
  }

  public byte[] getDataIfExists(String path) {
    try {
      if (client.checkExists().forPath(path) != null) {
        return client.getData().forPath(path);
      }
    } catch (Exception e) {
      log.error("When checking for referenced path: " + path, e);
    }
    return null;
  }

  private List<String> getPaths(List<String> reqPaths, Function<String, String> pathMapper) {
    if (reqPaths == null)
      return Collections.emptyList();
    return reqPaths.stream().map(path -> {
      String fullPath = appPrefix + pathMapper.apply(path);
      try {
        if (client.checkExists().forPath(fullPath) == null)
          return fullPath;
      } catch (Exception e) {
        log.error("When checking if required path exists", e);
        exceptionHandler.accept(e);
        return fullPath;
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * Populator is complete when INIT_SUBPATH is created for the component.
   */
  @Setter
  private Consumer<String> completeCallback =
      path -> log.info("ZkConfigPopulator done: created {}", path);

  private void triggerComponentInitialization(String componentId) {
    try {
      if (!refPathValues.isEmpty()) {
        String confResolvedPath = appPrefix + componentId + CONFRESOLVED_SUBPATH;
        log.info("populateResolvedConfig: {}", confResolvedPath);

        byte[] data = SerializeUtils.serialize(refPathValues);
        client.create().forPath(confResolvedPath, data);
      }

      // trigger component to start
      String initPath = appPrefix + componentId + ZkComponentStarter.INIT_SUBPATH;
      client.create().forPath(initPath);
      completeCallback.accept(initPath);
    } catch (Exception e) {
      exceptionHandler.accept(e);
      throw new RuntimeException(e);
    }
  }

  private void watchForRequiredPaths(List<String> reqPaths,
      Consumer<String> componentStartedConsumer) {
    for (String path : reqPaths) {
      log.info("watchForRequiredComponent: {}", path);
      getAsync().with(WatchMode.successOnly).watched().checkExists().forPath(path).event()
          .thenAcceptAsync(evt -> {
            log.info("Got event for path={}: {}", path, evt);
            switch (evt.getType()) {
              case NodeCreated:
                componentStartedConsumer.accept(path);
                break;
              default:
                log.info("Ignoring event for path={}: {}", path, evt);
                watchForRequiredPaths(reqPaths, componentStartedConsumer);
                break;
            }
          });
    }
  }

  public void cleanup() throws Exception {
    log.info("cleanup: {}", appPrefix);
    ZkConnector.deletePath(client, appPrefix);
  }

  public static void main(String[] args) throws Exception {
    String propFile = (args.length > 0) ? args[0] : "configs.props";
    Configuration config = ConfigReader.parseFile(propFile);
    log.info("{}\n------", ConfigReader.toStringConfig(config, config.getKeys()));

    String zkConnectionString=config.getString(ConstantsZk.ZOOKEEPER_CONNECT);
    String zkStartupPathHome=config.getString(ConstantsZk.ZOOKEEPER_STARTUPPATH);
    Injector injector = Guice.createInjector(new GModuleZooKeeper(zkConnectionString, zkStartupPathHome));
    ZkConfigPopulator cp = injector.getInstance(ZkConfigPopulator.class);

    boolean cleanUpOnly = Arrays.asList(args).contains("clean");
    if (cleanUpOnly) {
      cp.cleanup();
    } else {
      Consumer<Exception> exceptionHandler = e -> {
        throw new RuntimeException(e);
      };
      cp.populateConfigurations(propFile, exceptionHandler);
      log.info("Tree after config: {}", ZkConnector.treeToString(cp.client, cp.appPrefix));
    }
  }

  @Setter
  private Consumer<Exception> exceptionHandler=(e)->{};

  public static final String COMPONENT_IDS = "componentIds";
  public List<String> populateConfigurations(String propFile, Consumer<Exception> exceptionHandler)
      throws InterruptedException, ConfigurationException {
    Configuration config = ConfigReader.parseFile(propFile);
    String componentIds = System.getProperty(COMPONENT_IDS);
    if(componentIds==null || componentIds.length()==0)
      componentIds=config.getString(COMPONENT_IDS, "");
    List<String> compIdList =
        Arrays.stream(componentIds.split(",")).map(String::trim).filter(s->s.length()>0).collect(Collectors.toList());
    populateConfigurations(config, compIdList);
    return compIdList;
  }
  public void populateConfigurations(Configuration config, List<String> compIdList)
      throws InterruptedException {
    Map<String, Configuration> subConfigMap = ConfigReader.extractSubconfigMap(config);
    log.info("componentIds available in config file: {}", subConfigMap.keySet());

    if (compIdList.isEmpty())
      compIdList = new ArrayList<>(subConfigMap.keySet());
    log.info("ZKCONFIG: Component configurations to put in ZK: {}", compIdList);

    completeLatch = new CountDownLatch(compIdList.size());
    setCompleteCallback(p -> completeLatch.countDown());

    compIdList.forEach(compId -> {
      Configuration subconfig = subConfigMap.get(compId);
      if(subconfig==null) {
        log.warn("!! No configuration found for {}", compId);
        subconfig=new BaseConfiguration();
      }
      try {
        populateConfig(compId, subconfig);
      } catch (Exception ex) {
        log.error("When populating config for " + compId, ex);
        exceptionHandler.accept(ex);
      }
      triggerInitializationWhenReady(compId, subconfig);
    });
    
    long compsStillRunning = completeLatch.getCount();
    do {
      log.info("Waiting for required components to start before initiating other components: {}", compsStillRunning);
      completeLatch.await(secondsToWaitForRequiredComps, TimeUnit.SECONDS);
      compsStillRunning = completeLatch.getCount();
    } while (compsStillRunning > 0);
    
    log.info("ZKCONFIG: Done populating configs for: {}", compIdList);
  }
  
  CountDownLatch completeLatch;
  @Setter
  int secondsToWaitForRequiredComps=2;
}
