/**
 * 
 */
package net.deelam.activemq.rpc;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * @author dnlam
 *
 */
public interface HdfsSvcI {
  
  CompletableFuture<File> downloadFile(String src, String dst) throws IOException;

  CompletableFuture<Boolean> exists(String src) throws IOException;
  
  CompletableFuture<String> uploadFile(String localFile, String destPath, boolean overwrite) throws IllegalArgumentException, IOException;

  CompletableFuture<MyBean> getBean();

  CompletableFuture<MyBean> getBean2(); 

  CompletableFuture<List<MyBean>> getBeans();

  CompletableFuture<List<MyBean>> repeatBeans(List<MyBean> list);
  
  CompletableFuture<MyBean[]> getBeansArray();

  CompletableFuture<ComplexBean> getComplexBean();
  
  CompletableFuture<ComplexBean> repeatComplexBean(ComplexBean cBean);
  
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
    
    List<MyBean> list;
  }

  CompletableFuture<OffsetDateTime> getOffsetDateTime();

  Future<ComplexBean> repeatComplexBeanInt(ComplexBean myCBean, int i, String clientName);
}
