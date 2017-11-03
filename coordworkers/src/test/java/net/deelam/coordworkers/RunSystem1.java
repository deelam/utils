package net.deelam.coordworkers;

import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ZkComponentStarter;
import net.deelam.zkbasedinit.ZkComponentStopper;
import net.deelam.zkbasedinit.ZkConfigPopulator;
import net.deelam.zkbasedinit.ZkConnector;

/**
 * <pre>
 * Start ZK and populate
 * Start components:
 * - ActiveMQ service: shares connectionUrl
 * - Submitter: sends to submitJob queue; listens to jobStatus topic
 * - Jobber: shares submitJob queue and availableJobs topic names; listens to jobStatus topic
 * - Worker: listens on workers and availableJobs topics; publishes to jobStatus topic
 *
 * </pre>
 */
@Slf4j
public class RunSystem1 {

  public static void main(String[] args) throws Exception {
    // in JVM 1, Start ZK and populate
    new Thread(() -> {
      try {
        ZkConfigPopulator.main(new String[] {"configs.props"});
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "addConfigToZK").start();

    // in JVM 2, Start components
    new Thread(() -> {
      try {
        System.setProperty("componentIds","amq, submitterA, jobberB, workerType");
        ZkComponentStarter.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "startComponents").start();

    Thread.sleep(5000);
    // optionally
    new Thread(() -> {
      try {
        ZkConnector.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "printTree").start();

    Thread.sleep(5000);

    // in JVM 3
    new Thread(() -> {
      try {
        ZkComponentStopper.main(args);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "stopComponents").start();

    log.info("Done");
  }

}
