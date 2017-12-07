package net.deelam.vertx;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.inject.AbstractModule;

import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class ClusteredVertxInjectionModule extends AbstractModule {
  final CompletableFuture<Vertx> vertxF;
  final ClusteredVertxConfig vertxConfig = new ClusteredVertxConfig();

  @Override
  protected void configure() {
    // create clustered VertX
    if (!vertxF.isDone()) {
      // set cluster IPs from file
      try (FileInputStream is = new FileInputStream("vertx.props")) {
        Properties props = new Properties();
        props.load(is);
      } catch (FileNotFoundException e) {
        log.info("VERTX: No vertx.props found.  Creating non-clustered Vertx instance.");
        vertxF.complete(Vertx.vertx());
      } catch (IOException e) {
        log.error("Error reading vertx.props", e);
      }

      if (!vertxF.isDone()) {
        new Thread(() -> // Is a separate thread needed since we wait for it later?
        StartVertx.createClustered(vertxConfig, vertx -> {
          //System.out.println("=========  Vert.x service registered");
          vertxF.complete(vertx);
          log.info("VERTX: VertX created");
        }) //
        , "clusteredVertxCreator").start();
      }
    }

    try {
      boolean haveToWait = !vertxF.isDone();
      if (haveToWait)
        log.info("VERTX: Waiting for Vertx ... {}", vertxF);
      bind(Vertx.class).toInstance(vertxF.get(10, TimeUnit.SECONDS));
      if (haveToWait)
        log.info("VERTX: Got Vertx");
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

}
