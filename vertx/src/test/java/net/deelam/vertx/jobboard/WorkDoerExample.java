package net.deelam.vertx.jobboard;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graph.GrafUri;
import net.deelam.graph.IdGrafFactoryTinker;

@Slf4j
public class WorkDoerExample {

  public static void main(String[] args) throws InterruptedException {
    Vertx vertx = Vertx.vertx();

//    final CountDownLatch deployLatch = new CountDownLatch(4); // market, producer, consumer, listener
    Handler<AsyncResult<String>> deployHandler = res -> {
//      System.out.println(deployLatch + " ----- " + res.succeeded());
//      deployLatch.countDown();
    };

    ///---------  JobMarket
    final String svcType = "myJobMarket-"+WorkDoerExample.class.getSimpleName(); // connects JobConsumer and JobProducer to JobMarket
    {
      JobBoard jMarket = new JobBoard(svcType, svcType+System.currentTimeMillis());
      vertx.deployVerticle(jMarket, deployHandler);
    }

    ///---------  JobConsumer
    {
      // create Doer 
      Doer workDoer = new Doer();
      // connect with ReportingWorker; called by JobConsumer's thread pool
      ReportingWorker rw = new ReportingWorker(workDoer, (j)->true, () -> workDoer.state)
          .setProgressMonitorFactory(new VertxProgressMonitor.Factory(vertx));

      JobConsumer jConsumer = new JobConsumer(svcType, JOB_TYPE).setWorker(rw);
      vertx.deployVerticle(jConsumer, deployHandler);
    }

    ////---------  Do this during runtime
    /// create and submit a job
    {
      JobProducer jProducer = new JobProducer(svcType);
      vertx.deployVerticle(jProducer, deployHandler);

      IdGrafFactoryTinker.register();
      GrafUri guri = new GrafUri("tinker:///");
      IdGraph<?> dependencyGraph = guri.openIdGraph();
      DepJobService depJobMgr = new DepJobService(dependencyGraph, ()->jProducer);

      JobSubmitter submitter = new JobSubmitter(depJobMgr);

      /// create listener for job progress
      //JobProgressListener listener = new JobProgressListener(PROGRESS_LISTENER_ADDR);
      vertx.deployVerticle(submitter.listener, deployHandler);

//      deployLatch.await();
      submitter.submit();

      /// wait for job to finish
      System.out.println("Waiting");
      submitter.listener.jobDoneLatch.await();
    }

    vertx.close();
    System.out.println("Done");
  }

  /// ==== Job submitter
  private static final String JOB_TYPE = "MY_JOB_TYPE";

  @Accessors(chain = true)
  @Data
  static class Request {
    String id;
  }

  @RequiredArgsConstructor
  static class JobSubmitter {
    //    final 
    JobProducer jobProducer;
    final DepJobService depJobMgr;
    final String jobListenerAddr = "listenerAddr";


    void submit() {
      JobDTO job = new JobDTO("jsonJobId", JOB_TYPE, new Request().setId("reqId1")).progressAddr(jobListenerAddr,1000);
      JobDTO job2 = new JobDTO("jsonJobId2", JOB_TYPE, new Request().setId("reqId2")).progressAddr(jobListenerAddr,1000);
      if (jobProducer == null) {
        depJobMgr.addJob(job);
        depJobMgr.addDepJob(job2, new String[]{job.getId()});
      } else {
        jobProducer.addJob(job);
        jobProducer.addJob(job2);
      }
    }

    /// ==== Job progress listener verticle (Job submitter)

    JobProgressListener listener = new JobProgressListener();

    @RequiredArgsConstructor
    class JobProgressListener extends AbstractVerticle {
      CountDownLatch jobDoneLatch = new CountDownLatch(2);

      @Override
      public void start() throws Exception {
        super.start();
        vertx.eventBus().consumer(jobListenerAddr, (Message<JsonObject> msg) -> {
          System.out.println("===> : " + msg.body());
          JsonObject jo = msg.body();
          if (jo.getInteger("percent") == 100)
            jobDoneLatch.countDown();
          msg.reply(1);
        });
      }
    }
  }

  /// ==== Does job and updates state

  @NoArgsConstructor
  static class Doer implements Consumer<JobDTO> {
    ProgressState state = new ProgressState().setMetrics(new HashMap<>());

    @Override
    public void accept(JobDTO job) {
      try {
        Request req = (Request) job.getRequest();
        log.info("Params objects={}", req);

        state.setPercent(0).setMessage("Start job " + job.getId()).getMetrics().clear();
        Thread.sleep(2000);
        state.setPercent(50).setMessage("At 50%").getMetrics().put("A", 49);
        Thread.sleep(1000);
        state.setPercent(100).setMessage("Done 100%!").getMetrics().put("B", 99);
      } catch (DecodeException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /// ====

}
