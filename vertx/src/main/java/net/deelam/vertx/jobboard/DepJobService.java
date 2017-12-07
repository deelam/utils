package net.deelam.vertx.jobboard;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.deelam.graph.GrafTxn.begin;
import static net.deelam.graph.GrafTxn.commit;
import static net.deelam.graph.GrafTxn.rollback;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import com.tinkerpop.frames.FramedTransactionalGraph;

import io.vertx.core.eventbus.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graph.FramedGrafSupplier;
import net.deelam.graph.GrafTxn;
import net.deelam.vertx.jobboard.DepJobFrame.STATE;


@Slf4j
public class DepJobService implements DepJobService_I {

  private final FramedTransactionalGraph<TransactionalGraph> graph;
  
  private final Supplier<JobProducer> jobProdS;
  
  @Getter(lazy=true)
  private final JobProducer jobProducer=getJobProducerWithMsgHandlers();
 
  boolean removeOnCompletion = true;
  boolean removeOnFailure = false;
  
  private JobProducer getJobProducerWithMsgHandlers(){
    log.info("Registering JobProducer's job completion handlers");
    final JobProducer jobProducer=jobProdS.get();
    jobProducer.addJobCompletionHandler((Message<JobDTO> msg) -> {
      log.info("DISPATCHER: Job complete: {}", msg.body());
      String jobId = msg.body().getId();
      if (removeOnCompletion)
        jobProducer.removeJob(jobId, null);
      DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
      //log.debug("all jobs: {}", this);
      jobDone(jobV);
      if(log.isDebugEnabled())
        log.debug("Done jobId={} \n {}", jobId, toStringRemainingJobs(DepJobFrame.STATE_PROPKEY));
    });
    jobProducer.addJobFailureHandler((Message<JobDTO> msg) -> {
      log.warn("DISPATCHER: Job failed: {}", msg.body());
      String jobId = msg.body().getId();
      if (removeOnFailure)
        jobProducer.removeJob(jobId, null);
      cancelJobsDependentOn(jobId, null);
      DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
      //log.debug("all jobs: {}", this);
      jobFailed(jobV);
      if(log.isDebugEnabled())
        log.debug("Failed jobId={} \n {}", jobId, toStringRemainingJobs(DepJobFrame.STATE_PROPKEY));
    });
    return jobProducer;
  }
    
  public DepJobService(IdGraph<?> dependencyGraph, Supplier<JobProducer> jobProdS) {
    Class<?>[] typedClasses = {DepJobFrame.class};
    FramedGrafSupplier provider = new FramedGrafSupplier(typedClasses);
    graph = provider.get(dependencyGraph);
    this.jobProdS=jobProdS;
  }

  public synchronized void close() {
    synchronized (graph) {
      log.info("Closing {}", this);
      graph.shutdown();
    }
  }

  @Getter
  private Map<String, JobDTO> waitingJobs = new ConcurrentHashMap<>();
  @Getter
  private Map<String, JobDTO> submittedJobs = new ConcurrentHashMap<>();
  @Getter
  private Map<String, JobDTO> unsubmittedJobs = new ConcurrentHashMap<>();

  public String toString() {
    return
    //("Submitted jobs="+submittedJobs.toString())+
    ("\n\tWaiting jobs=" + waitingJobs.toString()) +
        ("\n\tUnsubmitted jobs=" + unsubmittedJobs.toString());
    // return GraphUtils.toString(graph, 1000, "jobType", "state");
  }

  public String toStringRemainingJobs(String... propsToPrint) {
    StringBuilder sb = new StringBuilder("Incomplete jobs:\n");
    GrafTxn.tryOn(graph, () -> {
      int nodeCount = 0;
      //log.info(GraphUtils.toString(graph, 100, "state", "order"));
      for (DepJobFrame jobV : graph.getVertices(DepJobFrame.TYPE_KEY, DepJobFrame.TYPE_VALUE,
          DepJobFrame.class)) {
        if (jobV.getState() != STATE.DONE) {
          ++nodeCount;
          Vertex n = jobV.asVertex();
          sb.append("  ").append(n.getId()).append(": ");
          sb.append(n.getPropertyKeys()).append("\n");
          if (propsToPrint != null && propsToPrint.length > 0) {
            String propValuesStr = toString(n, "\n    ", propsToPrint);
            if (propValuesStr.length() > 0)
              sb.append("    ").append(propValuesStr).append("\n");
          }
        }
      }
      sb.append(" (").append(nodeCount).append(" incomplete jobs)");
    });
    return (sb.toString());
  }

  private static String toString(Element n, String delim, String... propsToPrint) {
    StringBuilder sb = new StringBuilder();
    if (propsToPrint != null) {
      if (propsToPrint.length == 0) {
        propsToPrint = (String[]) n.getPropertyKeys().toArray(propsToPrint);
      }
      boolean first = true;
      for (String propKey : propsToPrint) {
        if (n.getProperty(propKey) != null) {
          if (first) {
            first = false;
          } else {
            sb.append(delim);
          }
          sb.append(propKey).append("=").append(n.getProperty(propKey).toString());
        }
      }
    }
    return sb.toString();
  }

  public int counter = 0;

  public synchronized CompletableFuture<Boolean> addJob(JobDTO job) {
    return addJob(true, job);
  }
  public synchronized CompletableFuture<Boolean> addDepJob(JobDTO job, String[] inJobIds) {
    return addJob(true, job, inJobIds);
  }

  public synchronized CompletableFuture<Boolean> addJob(boolean addToQueue, JobDTO job, String... inJobIds) {
    String jobId = job.getId();
    log.info("DISPATCHER: addJob: {}", jobId);
    // add to graph
    GrafTxn.tryOn(graph, () -> {
      DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
      if (jobV == null) {
        jobV = graph.addVertex(jobId, DepJobFrame.class);
        //        jobV.setJobType(job.getJobType());
        jobV.setUpdatable(job.isUpdatable());
        jobV.setOrder(++counter);
        graph.commit();
      } else {
        throw new IllegalArgumentException("Job with id already exists: " + jobId);
      }
      addDependentJobs(jobV, inJobIds);

      if (addToQueue)
        addToQueue(job, jobV);
      else
        unsubmittedJobs.put(jobId, job);
    });
    return CompletableFuture.completedFuture(true);
  }

  private void addToQueue(JobDTO job, DepJobFrame jobV) {
    synchronized (graph) {
      if (isJobReady(jobV)) {
        log.info("DISPATCHER: Submitting jobId={}", jobV.getNodeId());
        submitJob(jobV, job);
      } else {
        log.info("DISPATCHER: Input to job={} is not ready; setting state=WAITING.", job.getId());
        putJobInWaitingArea(jobV, job);
      }
    }
  }

  public synchronized Collection<String> listJobs(DepJobFrame.STATE state) {
    Collection<String> col = new ArrayList<>();
    GrafTxn.tryOn(graph, () -> {
      if (state == null) {
        for (Vertex v : graph.getVertices()) {
          col.add((String) v.getId());
        }
      } else {
        for (Vertex v : graph.getVertices(DepJobFrame.STATE_PROPKEY, state.name())) {
          col.add((String) v.getId());
        }
      }
    });
    return col;
  }

  /**
   * If job is waiting, move job to end of queue.
   * If job has been submitted, 
   * @param jobId
   */
  public synchronized void reAddJob(String jobId) {
    log.info("DISPATCHER: reAddJob: {}", jobId);
    GrafTxn.tryOn(graph, () -> {
      // add to graph
      DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
      if (jobV == null) {
        throw new IllegalArgumentException("Job doesn't exist: " + jobId);
      }

      JobDTO job;
      if (jobV.getState() == null)
        job = unsubmittedJobs.get(jobId);
      else
        switch (jobV.getState()) {
          case CANCELLED:
          case DONE:
          case FAILED:
          case NEEDS_UPDATE:
            log.info("re-add job: {}", jobId);
            job = submittedJobs.get(jobId);
            break;
          case WAITING:
            jobV.setOrder(++counter);
            log.info("re-add job: {} is currently waiting; adjusting order to {}", jobId, counter);
            return;
          case SUBMITTED:
          case PROCESSING:
          default:
            throw new IllegalStateException("Cannot re-add job " + jobId + " with state=" + jobV.getState());
        }

      addToQueue(job, jobV);
    });
  }

  public synchronized void addDependentJobs(String jobId, String... inJobIds) {
    log.info("DISPATCHER: addDependentJobs jobId={}", jobId);
    synchronized (graph) {
      GrafTxn.tryOn(graph, () -> {
        DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
        if (jobV == null) {
          throw new IllegalArgumentException("Job with id does not exist: " + jobId);
        } else {
          if (jobV.getState() != null)
            switch (jobV.getState()) {
              case CANCELLED:
              case DONE:
              case FAILED:
              case SUBMITTED:
              case PROCESSING:
                log.warn("Job={} in state={}; adding dependent jobs at this point may be ineffectual: {}", jobV,
                    jobV.getState(), inJobIds);
                break;
              default:
                // okay
            }
          //log.info("addInputJobs " + jobId);
          addDependentJobs(jobV, inJobIds);
        }
      });
    }
  }

  public boolean hasJob(String jobId) {
    return GrafTxn.tryOn(graph, (graph) -> {
      Vertex inputJobV = graph.getVertex(jobId);
      boolean exists = (inputJobV != null);
      return exists;
    });
  }

  private void addDependentJobs(DepJobFrame jobV, String... inJobIds) {
    if (inJobIds != null) {
      for (String inputJobId : inJobIds) {
        DepJobFrame inputJobV = graph.getVertex(inputJobId, DepJobFrame.class);
        if (inputJobV == null)
          throw new IllegalArgumentException("Unknown input jobId=" + inputJobId);
        if (!Iterables.contains(jobV.getInputJobs(), inputJobV))
          jobV.addInputJob(inputJobV);
      }
      graph.commit();
    }
  }

  public synchronized boolean cancelJob(String jobId) {
    log.info("DISPATCHER: cancelJob: {}", jobId);
    return GrafTxn.tryOn(graph, (graph) -> {
      DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
      if (jobV == null)
        throw new IllegalArgumentException("Unknown jobId=" + jobId);
      if (jobV.getState() == null) {
        unsubmittedJobs.remove(jobId);
        setJobCancelled(jobV);
      } else {
        switch (jobV.getState()) {
          case CANCELLED:
          case FAILED:
          case NEEDS_UPDATE:
          case DONE:
            log.info("Not cancelling {}. Job's current state={}", jobId, jobV.getState());
            break;
          case PROCESSING:
            log.info("Not cancelling {}. Job's current state={}.  Ask jobProcessor to cancel job.", jobId,
                jobV.getState());
            break;
          case WAITING:
            waitingJobs.remove(jobV.getNodeId());
            setJobCancelled(jobV);
            break;
          case SUBMITTED:
            log.info("Attempting to cancel submitted job={}", jobId);
            getJobProducer().removeJob(jobId, null); // may fail
            submittedJobs.remove(jobId);
            setJobCancelled(jobV);
            break;
        }
      }
      return true;
    });
  }

  private void setJobCancelled(DepJobFrame jobV) {
    synchronized (graph) {
      jobV.setState(STATE.CANCELLED);
      log.info("Cancelled job={}", jobV);
      graph.commit();
    }
  }

  public void cancelJobsDependentOn(String jobId, List<String> canceledJobs) {
    log.info("DISPATCHER: cancelJobsDependentOn failedJob: {}", jobId);
    GrafTxn.tryOn(graph, (graph) -> {
      DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
      for (DepJobFrame outJ : jobV.getOutputJobs()) {
        boolean cancelled = cancelJob(outJ.getNodeId());
        if (canceledJobs != null && cancelled)
          canceledJobs.add(outJ.getNodeId());
        cancelJobsDependentOn(outJ.getNodeId(), canceledJobs);
      }
    });
  }

  private void submitJob(DepJobFrame jobV, JobDTO job) {
    synchronized (graph) {
      log.debug("submitJob: {}", jobV);
      jobV.setState(STATE.SUBMITTED);
      graph.commit();
    }

    //log.debug("----------------------  submitJob: {} {}", job.getClass(), job);
    if (submittedJobs.containsKey(jobV.getNodeId())) {
      // NEEDED?: jobProd.removeJob(jobV.getNodeId(), null);
    }
    submittedJobs.put(jobV.getNodeId(), job);
    getJobProducer().addJob(job);
  }

  public STATE getJobStatus(String jobId) {
    return GrafTxn.tryOn(graph, (graph) -> {
      DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
      checkNotNull(jobV, "Cannot find " + jobId);
      return jobV.getState();
    });
  }

  private ExecutorService threadPool = Executors.newCachedThreadPool();

  public Map<String, Object> queryJobStats(String jobId) {
    int tx = begin(graph);
    try {
      DepJobFrame jobV = graph.getVertex(jobId, DepJobFrame.class);
      checkNotNull(jobV, "Cannot find " + jobId);

      Map<String, Object> map = new HashMap<>();
      if (jobV.getState() == STATE.PROCESSING) {
        threadPool.execute(() -> {
          getJobProducer().getProgress(jobId, reply -> {
            synchronized (map) {
              JobDTO bodyJO = reply.result().body();
              //job = Json.decodeValue(bodyJO.toString(), DependentJob.class);
              //Do we need to do this?: bodyJO.getRequest().forEach(e->map.put(e.getKey(), e.getValue()));
              //new JsonObject(Json.encode(bodyJO.getRequest()))
              map.notify();
            }
          });
        });

        // wait for reply
        synchronized (map) {
          while (map.size() == 0)
            try {
              map.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          commit(tx);
          return map;
        }
      } else {
        map.put("_jobState", jobV.getState());
        //map.put("_jobProgress", jobV.getProgress());
        commit(tx);
        return map;
      }
    } catch (RuntimeException re) {
      rollback(tx);
      throw re;
    }
  }

  private void putJobInWaitingArea(DepJobFrame jobV, JobDTO job) {
    synchronized (graph) {
      jobV.setState(STATE.WAITING);
      graph.commit();
    }
    waitingJobs.put(jobV.getNodeId(), job);
    log.info("{} Waiting jobs: {}", waitingJobs.size(), waitingJobs.keySet());
  }

  private boolean isJobReady(DepJobFrame jobV) {
    synchronized (graph) {
      for (DepJobFrame inV : jobV.getInputJobs()) {
        if (inV.getState() != STATE.DONE)
          return false;
      }
      graph.commit();
    }
    return true;
  }

  private void jobDone(DepJobFrame job) {
    synchronized (graph) {
      job.setState(STATE.DONE);
      markDependants(job);
      graph.commit();
    }
  }

  private void jobFailed(DepJobFrame job) {
    synchronized (graph) {
      job.setState(STATE.FAILED);
      graph.commit();
    }
  }

  private void markDependants(DepJobFrame doneJob) {
    /**
     * When a task is DONE, it iterate through each dependee (i.e., task that it feeds):
    If dependee is DONE, change its state to NEEDS_UPDATE if it is not QUEUED.
    If dependee is QUEUED, then don't do anything (it will be processed once popped from queue).
    If dependee NEEDS_UPDATE, do nothing; Client would have to check for task with this state after all submitted relevant requests are complete.
    If dependee is WAITING (was popped earlier), put it in head of queue to be assessed next and if possible processed.
     */
    synchronized (waitingJobs) {
      Iterable<DepJobFrame> outJobs = doneJob.getOutputJobs();
      if (outJobs != null) {
        SortedSet<DepJobFrame> readyJobs = new TreeSet<>((e1, e2) -> {
          return Integer.compare(e1.getOrder(), e2.getOrder());
        });
        for (DepJobFrame outV : outJobs) {
          if (outV.getState() == null) {
            log.info("Not marking job={} as NEEDS_UPDATE since job's current state={} (hasn't been submitted)",
                outV.getNodeId(), outV.getState());
          } else {
            switch (outV.getState()) {
              case SUBMITTED:
              case DONE:
                if(outV.getUpdatable()){
                  log.info("Job dependent on {} has state={}. Marking {} as NEEDS_UPDATE.", doneJob.getNodeId(),
                      outV.getState(), outV.getNodeId());
                  outV.setState(STATE.NEEDS_UPDATE);
                }
                break;
              case NEEDS_UPDATE:
                break;
              case WAITING:
                if (isJobReady(outV))
                  readyJobs.add(outV);
                break;
              case PROCESSING:
                if(outV.getUpdatable()){
                  log.info("Job dependent on {} is currently processing. Marking {} as NEEDS_UPDATE.",
                      doneJob.getNodeId(), outV.getNodeId());
                  outV.setState(STATE.NEEDS_UPDATE);
                }
                break;
              case CANCELLED:
              case FAILED:
                log.info("Not marking job={} as NEEDS_UPDATE since job's current state={}", outV.getNodeId(),
                    outV.getState());
                break;
            }
          }
        }
        for (DepJobFrame readyV : readyJobs) { // submit in order
          log.info("Waiting job is now ready; submitting: {}", readyV.getNodeId());
          JobDTO job = waitingJobs.remove(readyV.getNodeId());
          submitJob(readyV, job);
        }
        log.debug("Waiting jobs: {}", waitingJobs.keySet());
      }
    }
  }

}
