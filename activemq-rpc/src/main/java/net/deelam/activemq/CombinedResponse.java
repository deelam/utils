package net.deelam.activemq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@AllArgsConstructor 
@Getter
@ToString
@Slf4j
public class CombinedResponse<T> {
  final int expectedNumResponses;
  final CompletableFuture<List<T>> future=new CompletableFuture<>();
  List<T> responses=new ArrayList<>();
  String correlationID="combinedResponses-"+nextCorrelId();
  
  private static int privateCorrelationIdCounter=0;
  @Synchronized
  static int nextCorrelId() {
    return ++privateCorrelationIdCounter;
  }

  public boolean addResponse(T resp) {
    responses.add(resp);
    log.info("Received response {} of {}", responses.size(), expectedNumResponses);
    if(responses.size() == expectedNumResponses) {
      future.complete(responses);
    }
    return future.isDone();
  }
}