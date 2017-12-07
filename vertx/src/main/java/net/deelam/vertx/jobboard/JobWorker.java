package net.deelam.vertx.jobboard;

public interface JobWorker {

  boolean apply(JobDTO jobDto) throws Exception;

  default boolean canDo(JobDTO jobDto) {
    return true;
  }

}
