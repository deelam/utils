package net.deelam.coordworkers;

import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ComponentI;

@Slf4j
public class WorkerComp implements ComponentI {

  @Getter
  private boolean running = true;

  public String getComponentId() {
    return config.componentId;
  }

  SubmitterCompConfig config;

  class SubmitterCompConfig extends AbstractCompConfig {

    final String brokerUrl;
    final String submitJobQueue;
    final String jobStateTopic;
    final String jobDoneTopic;
    final String jobFailedTopic;
    final String availJobsTopic;

    // populate and print remaining unused properties
    public SubmitterCompConfig(Properties props) {
      super(props);
      brokerUrl = useRequiredRefProperty(props, "brokerUrl.ref");
      submitJobQueue = useRequiredProperty(props, "msgQ.submitJob");
      jobStateTopic = useRequiredProperty(props, "msgT.jobState");
      jobDoneTopic = useRequiredProperty(props, "msgT.jobDone");
      jobFailedTopic = useRequiredProperty(props, "msgT.jobFailed");
      availJobsTopic = useRequiredProperty(props, "msgT.availJobs");
      checkRemainingProps(props);
    }

  }

  @Override
  public void start(Properties configMap) {
    config = new SubmitterCompConfig(configMap);
    log.info("Starting component '{}' with: {}", config.componentId, configMap);
  }

  @Override
  public boolean reinit(Properties configMap) {
    log.info("Reinitializing component '{}' with: {}", getComponentId(), configMap);
    return true;
  }

  @Override
  public void stop() {
    log.info("Stopping component: {}", getComponentId());
    running = false;
  }


}
