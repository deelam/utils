package net.deelam.vertx.rpc;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

public interface SvcInterface {
  CompletableFuture<File> downloadFile(String src, String dst) throws IOException;

  CompletableFuture<Boolean> exists(String src) throws IOException;
  
  CompletableFuture<String> uploadFile(String localFile, String destPath, boolean overwrite) throws IllegalArgumentException, IOException;

  //void downloadFile(String src, String dst, Handler<AsyncResult<File>> handler) throws IOException;
  
  CompletableFuture<MyBean> getBean();

  CompletableFuture<MyBean> getBean2(); 

  CompletableFuture<List<MyBean>> getBeans();

  CompletableFuture<MyBean[]> getBeansArray();

  CompletableFuture<ComplexBean> getComplexBean();

  CompletableFuture<OffsetDateTime> getOffsetDateTime();

  //@Data
  @ToString
  @AllArgsConstructor
  public static class MyBean{
    String name;
    File file;
  }
  
  @Accessors(chain=true)
  @ToString
  //@Data
  public static class ComplexBean{
    @Setter
    MyBean innerBean;
  }
}
