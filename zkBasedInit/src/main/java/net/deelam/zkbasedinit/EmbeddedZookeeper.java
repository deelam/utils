package net.deelam.zkbasedinit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Properties;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.utils.PropertiesUtil;

@Slf4j
public class EmbeddedZookeeper {
  public static void main(String[] args) throws Exception {
    Properties props = getDefaultProperties();


    new EmbeddedZookeeper(props).runServer();
  }

  public static Properties getDefaultProperties() {
    Properties props = new Properties();
    // set defaults
    props.setProperty("tickTime", "2000");
    props.setProperty("dataDir", "./zookeeper-data");
    props.setProperty("clientPort", "2181");
    // props.setProperty("zookeeper.admin.enableServer", "false");
    props.setProperty("admin.serverPort", "8010"); // http://localhost:8010/commands
    return props;
  }

  @Getter
  private final ZooKeeperServerMain zooKeeperServer;
  private final ServerConfig configuration;

  public EmbeddedZookeeper(String configFile) throws IOException, ConfigException {
    this(new Properties(), configFile);
  }

  public EmbeddedZookeeper(Properties props) throws IOException, ConfigException {
    this(props, null);
  }

  EmbeddedZookeeper(Properties props, String configFile) throws IOException, ConfigException {
    if (configFile != null)
      try {
        PropertiesUtil.loadProperties(configFile, props);
      } catch (FileNotFoundException e) {
        log.warn("Using default properties since {}", e.getMessage());
      }

    // Using QuorumPeerMain is only for Multi Server (Clustered) Zookeeper
    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    quorumConfiguration.parseProperties(props);

    configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);
    zooKeeperServer = new ZooKeeperServerMain();
  }

  public String getConnectionString() {
    InetSocketAddress addr = configuration.getClientPortAddress();
    return addr.getHostString()+":"+addr.getPort();
  }
  
  /**
   * blocks until Zookeeper shuts down
   * @throws IOException
   * @throws AdminServerException
   */
  public synchronized void runServer() throws IOException, AdminServerException {
    zooKeeperServer.runFromConfig(configuration);
  }

  public Thread startAsThread(String threadName) throws IOException, ConfigException {
    Thread t = new Thread(() -> {
      try {
        runServer();
      } catch (Exception e) {
        log.error("When starting ZooKeeper", e);
      }
    }, threadName);
    t.start();
    return t;
  }

  public synchronized void stop() {
    // https://issues.apache.org/jira/browse/ZOOKEEPER-1873
    try {
      Method shutdown = ZooKeeperServerMain.class.getDeclaredMethod("shutdown");
      shutdown.setAccessible(true);
      shutdown.invoke(zooKeeperServer);
    } catch (Exception e) {
      log.error("When shutting down Zookeeper", e);
    }
  }

}
