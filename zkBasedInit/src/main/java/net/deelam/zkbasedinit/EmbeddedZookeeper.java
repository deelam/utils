package net.deelam.zkbasedinit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import lombok.extern.slf4j.Slf4j;
import net.deelam.utils.PropertiesUtil;

@Slf4j
public class EmbeddedZookeeper {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.setProperty("tickTime", "2000");
    props.setProperty("dataDir", "./zookeeper-data");
    props.setProperty("clientPort", "2181");
    // props.setProperty("zookeeper.admin.enableServer", "false");
    props.setProperty("admin.serverPort", "8010"); // http://localhost:8010/commands

    try {
      PropertiesUtil.loadProperties("zoo.cfg", props);
    } catch (FileNotFoundException e) {
      log.warn("Using default properties since {}", e.getMessage());
    }
    new EmbeddedZookeeper(props).runServer();
  }

  private final ZooKeeperServerMain zooKeeperServer;
  private final ServerConfig configuration;

  public EmbeddedZookeeper(Properties props) throws IOException, ConfigException {
    // Using QuorumPeerMain is only for Multi Server (Clustered) Zookeeper
    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    quorumConfiguration.parseProperties(props);

    configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);
    zooKeeperServer = new ZooKeeperServerMain();

  }

  public void runServer() throws IOException, AdminServerException {
    zooKeeperServer.runFromConfig(configuration);
  }

  public void startInThread() {
    new Thread(() -> {
      try {
        runServer();
      } catch (IOException | AdminServerException e) {
        log.error("ZooKeeper Failed", e);
      }
    }).start();
  }

}
