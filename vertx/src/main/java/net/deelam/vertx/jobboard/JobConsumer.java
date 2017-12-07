package net.deelam.vertx.jobboard;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.function.Function;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.KryoMessageCodec;
import net.deelam.vertx.jobboard.JobBoard.BUS_ADDR;
import net.deelam.vertx.rpc.ServiceWaiter;

@Slf4j
@Accessors(chain = true)
@RequiredArgsConstructor
@ToString
public class JobConsumer extends AbstractVerticle {
  private final String serviceType;
  private final String jobType;
  private DeliveryOptions deliveryOptions;

  @Override
  public void start() throws Exception {
    String myAddr = deploymentID();
    log.info("Ready: deploymentID={} jobType={}", deploymentID(), jobType);

    EventBus eb = vertx.eventBus();
    KryoMessageCodec.register(eb, JobDTO.class);
    KryoMessageCodec.register(eb, JobListDTO.class);

    deliveryOptions = JobBoard.createWorkerHeader(myAddr, jobType);

    eb.consumer(myAddr, jobListHandler);

    waiter = new ServiceWaiter(vertx, serviceType);
    waiter.listenAndBroadcast(msg -> {
      String jobBoardPrefix = msg.body();
      log.info("Sending client registration to {} from {}", jobBoardPrefix, myAddr);
      vertx.eventBus().send(jobBoardPrefix, null, deliveryOptions);
    });

  }

  @Getter(lazy = true)
  private final String jobBoardPrefix = waitUntilReady();
  private ServiceWaiter waiter;

  private String waitUntilReady() {
    return waiter.awaitServiceAddress();
  }

  private JobDTO pickedJob = null;

  @Setter
  private JobWorker worker;
  
  @Setter
  private Function<JobListDTO, JobDTO> jobPicker = dtoList -> {
    log.debug("jobs={}", dtoList);
    JobDTO picked = null;
    if (dtoList.jobs.size() > 0) {
      for(JobDTO j:dtoList.jobs){
        if((worker.canDo(j))){
          picked = j; //dto.jobs.get(0);
          break;
        }
      }
    }
    StringBuilder jobsSb = new StringBuilder();
    dtoList.jobs.forEach(j -> jobsSb.append(" " + j.getId()));
    log.info("pickedJob={} from jobs={}", picked, jobsSb);
    return picked;
  };

  @Setter
  private Handler<Message<JobListDTO>> jobListHandler = msg -> {
    try {
      checkState(pickedJob == null, "Job in progress! " + pickedJob);
      JobListDTO jobs = msg.body();
      pickedJob = jobPicker.apply(jobs);
    } finally {
      // reply immediately so conversation doesn't timeout
      // must reply even if picked==null
      msg.reply(pickedJob, deliveryOptions, ack -> {
        if (pickedJob != null) {
          if (ack.succeeded())
            doJob(pickedJob);
          else
            pickedJob = null; // job may have been removed while I was picking
        }
      });
    }
  };

  private void doJob(JobDTO pickedJob) {
    vertx.<Boolean>executeBlocking((successF) -> {
      try {
        successF.complete(worker.apply(pickedJob));
      } catch (Exception | Error e) {
        log.error("Worker " + worker + " threw exception; notifying job failed", e);
        successF.complete(false);
      }
    } , (res) -> {
      if (res.result()) {
        sendJobEndStatus(BUS_ADDR.DONE);
      } else {
        sendJobEndStatus(BUS_ADDR.FAIL);
      }
    });
  }
  
  private void sendJobEndStatus(BUS_ADDR method) {
    checkNotNull(pickedJob, "No job picked!");
    JobDTO doneJob = pickedJob;
    pickedJob = null; // set to null before notifying jobMarket, which will offer more jobs
    vertx.eventBus().send(getJobBoardPrefix() + method, doneJob, deliveryOptions);
  }
}
