package net.deelam.vertx.jobboard;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Data
@NoArgsConstructor(access=AccessLevel.PRIVATE)
public class JobDTO {
  String id;
  String type;
  Object request; // job-specific parameters
  
  public JobDTO(String id, String type, Object request) {
    this.id = id;
    this.type = type;
    this.request = request;
  }
  
  @Override
  public String toString() {
    return "JobDTO [id=" + id + ", type=" + type + "]";
  }  
  
  String progressVertxAddr; // Vertx eventbus address; job worker can register itself to this address
  int progressPollIntervalSeconds;

  public JobDTO progressAddr(String progressVertxAddr,int progressPollIntervalSeconds){
    this.progressVertxAddr=progressVertxAddr;
    this.progressPollIntervalSeconds=progressPollIntervalSeconds;
    return this;
  }
  
  boolean updatable=true;

}
