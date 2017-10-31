package net.deelam.zkbasedinit;

import java.util.concurrent.CountDownLatch;
import javax.inject.Named;
import org.apache.curator.framework.CuratorFramework;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GModuleZkComponentStarter extends AbstractModule {

  @Getter
  final CountDownLatch startedLatch;
  @Getter
  final CountDownLatch completedLatch;

  public GModuleZkComponentStarter(int compCount) {
    startedLatch = new CountDownLatch(compCount);
    completedLatch = new CountDownLatch(compCount);
  }

  @Override
  public void configure() {
    log.info("Using latches: {}, {}", startedLatch, completedLatch);
  }

  @Provides
  protected ZkComponentStarter createZkComponentStarter(CuratorFramework cf,
      @Named(Constants.ZOOKEEPER_STARTUPPATH) String startupPath) {
    ZkComponentStarter compStarter = new ZkComponentStarter(cf, startupPath);
    compStarter.setComponentStartedCallback(c -> startedLatch.countDown());
    compStarter.setStarterCompleteCallback(c -> completedLatch.countDown());
    return compStarter;
  }

  @Provides
  protected ZkComponentTypeStarter createZkComponentTypeStarter(CuratorFramework cf,
      @Named(Constants.ZOOKEEPER_STARTUPPATH) String startupPath) {
    ZkComponentTypeStarter compStarter = new ZkComponentTypeStarter(cf, startupPath);
    compStarter.setComponentStartedCallback(c -> startedLatch.countDown());
    compStarter.setStarterCompleteCallback(c -> completedLatch.countDown());
    return compStarter;
  }
}
