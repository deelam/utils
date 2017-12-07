package net.deelam.vertx.jobboard;

import java.util.function.Consumer;

public interface ProgressingDoer extends Consumer<JobDTO> {
  String name();

  ProgressState state();

  String jobType();

  boolean canDo(JobDTO job);
}
