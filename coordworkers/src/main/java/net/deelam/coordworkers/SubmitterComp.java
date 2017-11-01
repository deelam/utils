package net.deelam.coordworkers;

import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ComponentI;

@Slf4j
public class SubmitterComp implements ComponentI {

  @Getter
  private boolean running = true;

  @Getter
  private String componentId;

  SubmitterCompConfig config;

  class SubmitterCompConfig extends AbstractCompConfig {

    final String brokerUrl;
    final String submitJobQueue;
    final String jobStateTopic;
    final String jobDoneTopic;
    final String jobFailedTopic;

    // populate and print remaining unused properties
    public SubmitterCompConfig(Properties props) {
      super(props);
      brokerUrl = useRequiredRefProperty(props, "brokerUrl.ref");
      submitJobQueue = useRequiredProperty(props, "msgQ.submitJob");
      jobStateTopic = useRequiredProperty(props, "msgT.jobState");
      jobDoneTopic = useRequiredProperty(props, "msgT.jobDone");
      jobFailedTopic = useRequiredProperty(props, "msgT.jobFailed");
      checkRemainingProps(props);
    }
  }

  @Override
  public void start(Properties configMap) {
    componentId = configMap.getProperty(COMPONENT_ID);
    log.info("Starting component '{}' with: {}", componentId, configMap);
    config = new SubmitterCompConfig(configMap);
  }

  @Override
  public boolean reinit(Properties configMap) {
    log.info("Reinitializing component '{}' with: {}", componentId, configMap);
    return true;
  }

  @Override
  public void stop() {
    log.info("Stopping component: {}", componentId);
    running = false;
  }


}
