package net.deelam.activemq.rpc;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HdfsSvc implements HdfsSvcI {
  
  public void downloadFile(String src, String dst, java.util.function.Consumer<Future<File>> handler) throws IOException {
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
    handler.accept(CompletableFuture.completedFuture(new File(dst)));
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
  public CompletableFuture<List<MyBean>> repeatBeans(List<MyBean> list) {
    return CompletableFuture.completedFuture(list);
  }


  @Override
  public CompletableFuture<ComplexBean> repeatComplexBean(ComplexBean cBean) {
    return CompletableFuture.completedFuture(cBean);
  }


  @Override
  public CompletableFuture<OffsetDateTime> getOffsetDateTime() {
    return CompletableFuture.completedFuture(OffsetDateTime.now());
  }


  Map<String,AtomicInteger> clientMap=new HashMap<>();
  @Override
  public Future<ComplexBean> repeatComplexBeanInt(ComplexBean cBean, int i, String clientName) {
    //log.info("server received: {}", i);
    AtomicInteger prevI = clientMap.get(clientName);
    if(prevI==null){
      prevI=new AtomicInteger(i);
      clientMap.put(clientName, prevI);
      log.info("Adding new client {} at {}", clientName, prevI);
    } else if(i<0){
      log.info("Removing client "+clientName);
      clientMap.remove(clientName);
    } else if(prevI.get()>=i)
      log.error("##### Out of order: prevI="+prevI+" and got i="+i+" for client="+clientName);
    else if(prevI.get()+1!=i)
      log.error("##### Missed a msg: prevI="+prevI+" and got i="+i+" for client="+clientName);
    prevI.set(i);
    return CompletableFuture.completedFuture(cBean);
  }

}

