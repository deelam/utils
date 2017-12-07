package net.deelam.vertx;

import java.util.function.Consumer;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import com.google.common.base.Stopwatch;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StartVertx {

  public static void createClustered(ClusteredVertxConfig config, Consumer<Vertx> vertxCons) {
    createClustered(config, null, vertxCons);
  }

  public static void createClustered(ClusteredVertxConfig config, VertxOptions options, Consumer<Vertx> vertxCons) {
    if (options == null)
      options = new VertxOptions();
    Stopwatch sw = Stopwatch.createStarted();
    config.infer();
    log.info("createClustered: {}", config);
    ClusterManager clusterManager;
    switch (config.clusterMgr) {
      case IGNITE_ALL_SERVERS:
        clusterManager = createIgniteClusterManager(config);
        break;
      default:
        throw new IllegalArgumentException("Unknown: " + config.clusterMgr);
    }

    options.setClustered(true);
    options.setClusterManager(clusterManager);
    if (options.getClusterHost() == null || options.getClusterHost().equals("localhost")) {
      log.info("Setting options.setClusterHost={}", config.myIp);
      options.setClusterHost(config.myIp);
    }
    //options.setClusterPort(ipInfo.serverPort);

    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        log.info("Clustered Vertx instance created in {}", sw);
        Vertx vertx = res.result();
        vertxCons.accept(vertx);
      } else {
        log.error("Could not initialize Vertx", res.cause());
        //          res.cause().printStackTrace();
      }
    });

    //  Runtime.getRuntime().addShutdownHook(new Thread(()->{
    //  shutdown()
    //}, "vertx-shutdown-hook"));
  }

  private static ClusterManager createIgniteClusterManager(ClusteredVertxConfig config) {
    // Check to make sure settings are valid
    if ((config.tcpCommPort >= config.serverPort &&
        config.tcpCommPort <= config.getEndServerPort())) {
      log.error("Invalid tcp port {}.  Falls within range {}..{}",
          config.tcpCommPort, config.serverPort, config.getEndServerPort());
      throw new IllegalArgumentException();
    }

    IgniteConfiguration cfg = new IgniteConfiguration();

    // Explicitly configure TCP discovery SPI to provide list of initial nodes
    // from the first cluster.
    TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

    // Initial local port to listen to.
    discoverySpi.setLocalPort(config.serverPort);

    // Changing local port range. This is an optional action.
    discoverySpi.setLocalPortRange(config.portRangeSize);

    TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

    // Addresses and port range of the nodes from the first cluster.
    // 127.0.0.1 can be replaced with actual IP addresses or host names.
    // The port range is optional.
    ipFinder.setAddresses(config.clusterIpList());

    // Overriding IP finder.
    discoverySpi.setIpFinder(ipFinder);

    // Explicitly configure TCP communication SPI by changing local port number for
    // the nodes from the first cluster.
    TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

    commSpi.setLocalPort(config.tcpCommPort);

    // Overriding discovery SPI.
    cfg.setDiscoverySpi(discoverySpi);

    // Overriding communication SPI.
    cfg.setCommunicationSpi(commSpi);

    // Set cache for vertx-ignite
    cfg.setCacheConfiguration(new CacheConfiguration<>()
        .setName("*")
        .setBackups(1)
        .setCacheMode(CacheMode.PARTITIONED));

    System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, Boolean.FALSE.toString());
    System.setProperty(IgniteSystemProperties.IGNITE_NO_ASCII, Boolean.TRUE.toString());
    IgniteClusterManager cm = new IgniteClusterManager(cfg);
    return cm;
  }

}

