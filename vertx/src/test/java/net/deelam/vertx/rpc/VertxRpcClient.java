package net.deelam.vertx.rpc;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.ClusteredVertxConfig;
import net.deelam.vertx.StartVertx;
import net.deelam.vertx.rpc.SvcInterface.ComplexBean;
import net.deelam.vertx.rpc.SvcInterface.MyBean;

@Slf4j
public class VertxRpcClient {

  static Vertx vertx; // = Vertx.vertx();

  public static void main(String[] args) throws InterruptedException, ExecutionException {

    CompletableFuture<Vertx> vertxF = new CompletableFuture<>();
    if (args.length == 0) {
      ClusteredVertxConfig config1 = new ClusteredVertxConfig();
      StartVertx.createClustered(config1, vertX -> {
        log.info("Vert.x service registered");
        vertxF.complete(vertX);

      });
    }

    vertx = vertxF.get();

    SvcInterface rpcClient = new VertxRpcUtil(vertx, VertxRpcServer.SVC_INBOX)
        .createClient(SvcInterface.class);

    try {
      go(rpcClient);
    } catch (Exception e1) {
      e1.printStackTrace();
    }

    try {
      Thread.sleep(1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("Done");


    //Thread.sleep(2000);
    System.exit(0); // doesn't cause exceptions, which occur if vertx.close() is called

    {
      CompletableFuture<String> closed = new CompletableFuture<>();
      vertx.close((h) -> closed.complete(h.toString()));
      closed.get();
    }
    System.out.println("Closed");
    //Thread.sleep(5000);

  }

  private static void go(SvcInterface rpcClient) throws Exception {
    if (true) {
      try {
        Future<Boolean> exists = rpcClient.exists("src");
        System.out.println("exists=" + exists.get());
        Future<String> up = rpcClient.uploadFile("localFile", "destPath", false);
        System.out.println("up=" + up.get());

        try {
          Future<MyBean> bean = rpcClient.getBean2();
          System.out.println("bean=" + bean.get());

          CompletableFuture<List<MyBean>> beans = rpcClient.getBeans();
          System.out.println("beans=" + beans.get());

          CompletableFuture<MyBean[]> beansArr = rpcClient.getBeansArray();
          System.out.println("beansArr=" + Arrays.toString(beansArr.get()));
        } catch (Exception e) {
          e.printStackTrace();
        }

        Future<ComplexBean> cbean = rpcClient.getComplexBean();
        System.out.println("complexbean=" + cbean.get());

        Future<File> ffile = rpcClient.downloadFile("src", "dst");
        System.out.println("file=" + ffile.get());

        //CompletableFuture<File> ffile2=new CompletableFuture<>(); 
        //rpcClient.downloadFile("src", "dst", (res)->ffile2.complete(res.result()));
        //System.out.println("file2="+ffile2.get());

        OffsetDateTime odt = rpcClient.getOffsetDateTime().get();
        System.out.println("time=" + odt);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
