package net.deelam.vertx.jobboard;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.KryoMessageCodec;
import net.deelam.vertx.jobboard.JobBoard.BUS_ADDR;
import net.deelam.vertx.rpc.ServiceWaiter;

@RequiredArgsConstructor
@Slf4j
@ToString
public class JobProducer extends AbstractVerticle {
  @Getter
  private final String serviceType;

  private Context eventLoopContext;

  @Override
  public void start() throws Exception {
    eventLoopContext = context;

    KryoMessageCodec.register(vertx.eventBus(), JobDTO.class);
    KryoMessageCodec.register(vertx.eventBus(), JobListDTO.class);

    waiter = new ServiceWaiter(vertx, serviceType);
    waiter.listenAndBroadcast();

    log.info("Ready: deploymentID={} JobProducer for: {}", deploymentID(), serviceType);
  }

  @Getter(lazy = true)
  private final String jobBoardPrefix = waitUntilReady();
  private ServiceWaiter waiter;

  private String waitUntilReady() {
    return waiter.awaitServiceAddress();
  }

  private String jobCompletionAddress = null;
  private String jobFailureAddress = null;

  private void waitForEventBus() {
    while (vertx == null || vertx.eventBus() == null)
      try {
        log.info("Waiting for Vertx.eventBus to be initialized for " + this);
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting", e);
      }
  }

  public <T> void addJobCompletionHandler(Handler<Message<JobDTO>> jobCompletionHandler) {
    waitForEventBus();
    jobCompletionAddress = deploymentID() + "-jobComplete";
    log.info("add jobCompletionHandler to address={}", jobCompletionAddress);
    vertx.eventBus().consumer(jobCompletionAddress, jobCompletionHandler);
  }

  public <T> void addJobFailureHandler(Handler<Message<JobDTO>> jobFailureHandler) {
    waitForEventBus();
    jobFailureAddress = deploymentID() + "-jobFailed";
    log.info("add jobFailureHandler to address={}", jobFailureAddress);
    vertx.eventBus().consumer(jobFailureAddress, jobFailureHandler);
  }

  public void addJob(JobDTO job) {
    eventLoopContext.runOnContext((v) -> {
      DeliveryOptions deliveryOpts = JobBoard.createProducerHeader(jobCompletionAddress, jobFailureAddress, 0);
      vertx.eventBus().send(getJobBoardPrefix() + BUS_ADDR.ADD_JOB, job, deliveryOpts, addJobReplyHandler);
    });
  }

  public void removeJob(String jobId, Handler<AsyncResult<Message<JobDTO>>> removeJobReplyHandler) {
    eventLoopContext.runOnContext((v) -> {
      vertx.eventBus().send(getJobBoardPrefix() + BUS_ADDR.REMOVE_JOB, jobId,
          (removeJobReplyHandler == null) ? this.removeJobReplyHandler : removeJobReplyHandler);
    });
  }

  public void getProgress(String jobId, Handler<AsyncResult<Message<JobDTO>>> handler) {
    eventLoopContext.runOnContext((v) -> {
      vertx.eventBus().send(getJobBoardPrefix() + BUS_ADDR.GET_PROGRESS, jobId,
          (handler == null) ? this.progressReplyHandler : handler);
    });
  }

  @Setter
  private Handler<AsyncResult<Message<JobDTO>>> addJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("Job added: {}", reply.result().body());
    } else if (reply.failed()) {
      log.error("addJob failed: ", reply.cause());
    } else {
      log.warn("addJob unknown reply: {}", reply);
    }
  };

  @Setter
  private Handler<AsyncResult<Message<JobDTO>>> removeJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("Job removed: {}", reply.result().body());
    } else if (reply.failed()) {
      log.error("removeJob failed: ", reply.cause());
    } else {
      log.warn("removeJob unknown reply: {}", reply);
    }
  };

  @Setter
  private Handler<AsyncResult<Message<JobDTO>>> progressReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("Job progress: {}", reply.result().body());
    } else if (reply.failed()) {
      log.error("jobProgress failed: ", reply.cause());
    } else {
      log.warn("jobProgress unknown reply: {}", reply);
    }
  };
}
