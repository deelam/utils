package net.deelam.vertx;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MethodIdTest {

  @Before
  public void setUp() throws Exception {}

  public static interface ClassWithVarArgs{
    CompletableFuture<Boolean> noArgs();
    CompletableFuture<Boolean> oneArg1(String str);
    CompletableFuture<Boolean> oneArg1(Boolean str);
    
    // these 2 are not distinguishable by their .toString()
    CompletableFuture<Boolean> twoArgsArray(Boolean str, String[] strs);
    CompletableFuture<Boolean> twoArgsVarArgs(Boolean str, String... strs);

    // same method name distinguishable as long as diff num of params
    CompletableFuture<Boolean> twoArgsArray(Boolean str);
  }
  
  @Test
  public void test() {
    for (Method method : ClassWithVarArgs.class.getDeclaredMethods()) {
      method.setAccessible(true);
      log.info("{} \n\t {}", genMethodId(method), method);
    }
  }

  static String genMethodId(Method method) {
    StringBuilder sb=new StringBuilder(method.getName());
    sb.append(method.getParameterCount());
    for(Class<?> paramT:method.getParameterTypes()){
      sb.append(paramT.getSimpleName());
    }
    return sb.toString(); 
    // Doesn't work with varargs
  }
  
}
