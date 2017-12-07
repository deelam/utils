package net.deelam.vertx.jobboard;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true) // Vertx's eventbus doesn't work with fluents
public class ProgressState {
  int percent = 0;
  String message;

  public ProgressState(int percent, String message) {
    this.percent = percent;
    this.message = message;
  }

  public void starting(String jobId, Object object) {
    if (object == null)
      starting(jobId, null);
    else
      starting(jobId, object.toString());
  }

  public void done(Object object) {
    if (object == null)
      done(null);
    else
      done(object.toString());
  }

  private Map<String, Object> metrics = new ConcurrentHashMap<>();
  private String jobId;
  private long startTime;

  public ProgressState starting(String jobId, String msg) {
    this.jobId = jobId;
    if (metrics != null)
      metrics.clear();
    Instant now = Instant.now();
    metrics.put(START_TIME, now.toString());
    startTime = System.currentTimeMillis();
    setPercent(1);
    if (msg == null)
      msg="Starting " + jobId + " at "+ now;
    setMessage(msg);
    log.info("JOB: Starting: {}", msg);
    return this;
  }

  public ProgressState done(String msg) {
    setPercent(100);
    setElapsedTime();
    if (msg == null)
      setMessage(jobId+" done in "+((System.currentTimeMillis() - startTime)/1000)+" seconds.");
    else
      setMessage(msg);
    log.info("JOB: Done: {}", msg);
    return this;
  }

  public ProgressState failed(Throwable e) {
    return failed(e.getMessage());
  }

  public ProgressState failed(String msg) {
    if (getPercent() == 0)
      setPercent(-1);
    else if (getPercent() > 0)
      setPercent(-getPercent());
    setMessage(msg);
    log.warn("JOB: Failed {}: {}: {}", jobId, msg, metrics);
    setElapsedTime();
    return this;
  }

  
  private static final String START_TIME = "startTime";
  private static final String ELAPSED_MILLIS = "elapsedMillis";

  private void setElapsedTime() {
    if (metrics != null) {
      metrics.put(ELAPSED_MILLIS, System.currentTimeMillis() - startTime);
    }
  }

}
