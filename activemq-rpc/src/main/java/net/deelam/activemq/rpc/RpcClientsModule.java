package net.deelam.activemq.rpc;

import java.util.function.Supplier;
import javax.jms.Connection;
import com.google.inject.AbstractModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/// provides verticle clients used by REST services
@RequiredArgsConstructor
@Slf4j
public class RpcClientsModule extends AbstractModule {
  final Connection connection;

  @Override
  protected void configure() {
  }

  protected boolean debug = false;

  public <T> Supplier<T> getAmqClientSupplierFor(Class<T> clazz, String serverAddr) {
    log.info("AMQ: Creating supplier of RPC client for {}", clazz.getSimpleName());
    ActiveMqRpcClient client = new ActiveMqRpcClient(null, connection);
    return ()->client.createRpcClient(serverAddr, clazz, debug); // blocks?
  }
}
