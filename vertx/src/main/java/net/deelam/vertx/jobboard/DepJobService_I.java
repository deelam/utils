package net.deelam.vertx.jobboard;

import java.util.concurrent.CompletableFuture;

public interface DepJobService_I {

  CompletableFuture<Boolean> addJob(JobDTO job);
  
  CompletableFuture<Boolean> addDepJob(JobDTO job, String[] inJobIds);

}
