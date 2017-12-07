package net.deelam.vertx.rpc;



import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoteSvc implements SvcInterface {
  
  public void downloadFile(String src, String dst, Handler<AsyncResult<File>> handler) throws IOException {
    try {
      log.info("Working ...");
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    log.info("... done working");
    if (src==null)
      throw new IOException("test Exception");
    
    System.out.println("Calling handler "+handler.getClass());
    handler.handle(io.vertx.core.Future.succeededFuture(new File(dst)));
  }
  
  
  @Override
  public CompletableFuture<File> downloadFile(String src, String dst) throws IOException {
    try {
      log.info("Working ...");
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
      log.info("... done working");
   
    if (!true)
      throw new IOException("test Exception");
    
//    return CompletableFuture.completedFuture(src);
    return CompletableFuture.completedFuture(new File(src));
  }


  @Override
  public CompletableFuture<Boolean> exists(String src) throws IOException {
    log.info("called exists()");
    return CompletableFuture.completedFuture(Boolean.valueOf(true));
  }


  @Override
  public CompletableFuture<String> uploadFile(String localFile, String destPath, boolean overwrite)
      throws IllegalArgumentException, IOException {
    log.info("called uploadFile()");
    return CompletableFuture.completedFuture(localFile);
  }


  @Override
  public CompletableFuture<MyBean> getBean() {
    return CompletableFuture.completedFuture(new MyBean("myName", new File(".")));
  }

  @Override
  public CompletableFuture<MyBean> getBean2() {
    return CompletableFuture.completedFuture(new MyBean("2myName", new File(".")));
  }

  @Override
  public CompletableFuture<ComplexBean> getComplexBean() {
    return CompletableFuture.completedFuture(new ComplexBean().setInnerBean(new MyBean("myName", new File("."))));
  }


  @Override
  public CompletableFuture<List<MyBean>> getBeans() {
    List<MyBean> list=new ArrayList<>();
    list.add(new MyBean("myName", new File(".")));
    list.add(new MyBean("myName2", new File("2")));
    return CompletableFuture.completedFuture(list);
  }


  @Override
  public CompletableFuture<MyBean[]> getBeansArray() {
    List<MyBean> list=new ArrayList<>();
    list.add(new MyBean("myName", new File(".")));
    list.add(new MyBean("myName2", new File("2")));
    return CompletableFuture.completedFuture(list.toArray(new MyBean[2]));
  }


  @Override
  public CompletableFuture<OffsetDateTime> getOffsetDateTime() {
    OffsetDateTime offsetDateTime = OffsetDateTime.now().atZoneSameInstant(ZoneId.of("Z")).toOffsetDateTime();
    System.out.println(offsetDateTime);
    return CompletableFuture.completedFuture(offsetDateTime);
  }

}

