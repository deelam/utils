package net.deelam.activemq.rpc;

import java.util.function.Supplier;
import javax.jms.Connection;
import com.google.inject.AbstractModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// TODO: refactor class to be a Guice-created Factory (see JobListener_I.Factory)
/// provides RPC clients used by REST services
@RequiredArgsConstructor
@Slf4j
public class RpcClientsModule extends AbstractModule {
  final Connection connection;

  @Override
  protected void configure() {
  }

  protected boolean debug = false;

  public <T> Supplier<T> getAmqClientSupplierFor(Class<T> clazz, String serverAddr) {
    return getAmqClientSupplierFor(connection, clazz, serverAddr, debug);
  }
  
  public static <T> Supplier<T> getAmqClientSupplierFor(Connection connection, Class<T> clazz,
      String serverAddr, boolean debug) {
    log.info("AMQ: Creating supplier of RPC client for {}", clazz.getSimpleName());
    ActiveMqRpcClient client = new ActiveMqRpcClient(null, connection);
    return () -> client.createRpcClient(serverAddr, clazz, debug); // blocks?
  }
}
