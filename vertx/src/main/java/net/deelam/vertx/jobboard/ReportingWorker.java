package net.deelam.vertx.jobboard;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject) )
public class ReportingWorker implements JobWorker, HasProgress {

  private final Consumer<JobDTO> doer;

  private final Function<JobDTO,Boolean> canDoF;
  
  @Override
  public boolean canDo(JobDTO job) {
    return canDoF.apply(job);
  }
  
  @Override
  public boolean apply(JobDTO job) {
    try (ProgressMonitor pm = progressMonitorProvider.apply(job)) {
      pm.setProgressMaker(this);
      doer.accept(job);
      return true;
    } catch (Throwable e) {
      log.error("WORKER: Error during job processing by "+doer, e);
      ProgressState state = getProgress();
      if(state.getPercent()>0)
        log.warn("WORKER: Should set percent to negative! {}", state);
      return false;
    }
  }

  private final Supplier<ProgressState> progressSupplier;

  @Override
  public ProgressState getProgress() {
    return progressSupplier.get();
  }
  
  public ReportingWorker setProgressMonitorFactory(ProgressMonitor.Factory pmFactory) {
    if(progressMonitorProvider!=DUMMY_PM_PROVIDER)
      log.warn("WORKER: Overriding previously set progressMonitorProvider={}", progressMonitorProvider);
    progressMonitorProvider = job -> {
      return pmFactory.create(job.getId(), job.getProgressPollIntervalSeconds(), job.getProgressVertxAddr());
    };
    return this;
  }

  @Setter
  private Function<JobDTO, ProgressMonitor> progressMonitorProvider=DUMMY_PM_PROVIDER;

  private static Function<JobDTO, ProgressMonitor> DUMMY_PM_PROVIDER = job -> {
    return new ProgressMonitor() {
      public void close() throws Exception {}
      public void setProgressMaker(HasProgress process) {}
      public void update(ProgressState p) {}
      public void stop() {}
      public void addTargetVertxAddr(String vertxAddrPrefix) {}
    };
  };

}
