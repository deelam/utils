package net.deelam.activemq.rpc;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.CombinedResponse;

@Slf4j
@RequiredArgsConstructor
public class ActiveMqRpcUtil {
  final Session session;
  final String address;
  int deliveryMode = DeliveryMode.NON_PERSISTENT;
  final Map<String, CompletableFuture<Object>> returnObjs = new HashMap<>();

  @Setter
  RpcHook hook;

  private static final String HEADER_METHOD_ID = "method";
  private static final String EXCEPTION = "exception";

  private static int privateProxyIdCounter=0;
  @Synchronized
  static int nextProxyId() {
    return ++privateProxyIdCounter;
  }
  
  private static int privateCorrelationIdCounter=0;
  @Synchronized
  static int nextCorrelId() {
    return ++privateCorrelationIdCounter;
  }
  
  @SuppressWarnings("unchecked")
  public <T> T createClient(Class<T> iface) throws JMSException {
    final KryoSerDe serde = new KryoSerDe(session);

    Destination topic = session.createTopic(address + ".topic");
    MessageProducer topicMsgProducer = session.createProducer(topic);
    topicMsgProducer.setDeliveryMode(deliveryMode);

    Destination queue = session.createQueue(address + ".queue");
    MessageProducer queueMsgProducer = session.createProducer(queue);
    queueMsgProducer.setDeliveryMode(deliveryMode);

    final String clientId = iface.getSimpleName()+nextProxyId();
    Destination replyQueue = session.createQueue(address + ".replyQ." + clientId);
    MessageConsumer responseConsumer = session.createConsumer(replyQueue);
    responseConsumer.setMessageListener((msg) -> {
      try {
        String corrId = msg.getJMSCorrelationID();
        checkNotNull(corrId);
        CompletableFuture<Object> resultF = returnObjs.get(corrId);
        checkNotNull(resultF, corrId+" returnObjs="+returnObjs);
        log.debug("Got correlId={} result={}" ,corrId, resultF);

        if (msg instanceof BytesMessage) {
          BytesMessage bytesMsg = (BytesMessage) msg;
          String methodId = bytesMsg.getStringProperty(HEADER_METHOD_ID);
          if (bytesMsg.getBodyLength() == 0) {
            if (hook != null)
              hook.clientReceivedVoid(methodId);
            resultF.complete(null);
          } else {
            String exceptionStr = bytesMsg.getStringProperty(EXCEPTION);
            Object result = serde.readObject(bytesMsg);
            if (exceptionStr != null) {
              Throwable throwable = (result instanceof Throwable)
                  ? (Throwable) result
                  : new RuntimeException((result == null)
                      ? exceptionStr : result.toString());
              if (hook != null)
                hook.clientReceivedThrowable(methodId, throwable);
              resultF.completeExceptionally(throwable);
            } else {
              if (hook != null)
                hook.clientReceivesResult(methodId, result);
              resultF.complete(result);
            }
          }
        }
        if(returnObjs.remove(corrId)!=null)
          log.debug("Removed correlId={}" ,corrId);
      } catch (Exception e) {
        log.error("When reading response msg from service:", e);
      }

    });

    //    Context context = vertx.getOrCreateContext();
    //    log.info("vertxContext={} for client class={}", context, iface.getSimpleName());
    log.info("client class={}", iface.getSimpleName());
    //    if(context.get("dnlam")!=null)
    //      log.warn("Reusing context with deploymentId={}", context.deploymentID());
    //    context.put("dnlam","dnlam");
    //    checkNotNull(context);
    final String correlIdPrefix = "activemqRpc:"+clientId+":";
    return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[] {iface},
        (proxy, method, args) -> {
          try {
            String methodId = genMethodId(method);
            final BytesMessage msg = (method.getParameterTypes().length > 0) ? serde.writeObjects(args) : serde.writeObjects(null);
            final Class<?> returnType = method.getReturnType();
            msg.setStringProperty(HEADER_METHOD_ID, methodId);
            if (returnType == void.class) {
              if (hook != null)
                hook.clientSendsCall(methodId, args);
              if (method.getName().startsWith("publish"))
                topicMsgProducer.send(msg);
              else
                queueMsgProducer.send(msg);
              return null;
            } else if (returnType.isAssignableFrom(CompletableFuture.class)) {
              msg.setJMSReplyTo(replyQueue);
              String correlStr = correlIdPrefix+methodId+":"+Integer.toString(nextCorrelId());
              msg.setJMSCorrelationID(correlStr);
              final CompletableFuture<Object> resultF = new CompletableFuture<>();
              checkState(!returnObjs.containsKey(correlStr));
              returnObjs.put(correlStr, resultF);
              log.debug("Registered correlId={}, returnObjs={}",correlStr, returnObjs);
              queueMsgProducer.send(msg);
              log.debug("client sent {} with args={}", methodId, Arrays.toString(args));
              return resultF;
            } else if ("toString".equals(method.getName())) {
              return this.toString()+"("+iface.getName()+")";
            } else {
              throw new UnsupportedOperationException("Unsupported return type: " + returnType);
            }
          } catch (Throwable e) {
            log.error("Client-side error", e);
            return null;
          }
        });
  }

  public <T> void registerServer(T service) {
    log.info("Register server: {} at address={}", service.getClass().getName(), address);
    HashMap<String, Method> methods = new HashMap<>();
    for (Method method : service.getClass().getDeclaredMethods()) {
      method.setAccessible(true);
      methods.put(genMethodId(method), method);
    }
    final KryoSerDe serde = new KryoSerDe(session);
    log.info("server class={}", service.getClass().getSimpleName());

    MessageListener msgListener = msg -> {
      try {
        MessageProducer producer = session.createProducer(null);
        //Setup a message producer to respond to messages from clients, we will get the destination
        //to send to from the JMSReplyTo header field from a Message
        producer.setDeliveryMode(deliveryMode);

        if (msg instanceof BytesMessage) {
          BytesMessage bytesMsg = (BytesMessage) msg;
          String methodId = bytesMsg.getStringProperty(HEADER_METHOD_ID);
          if (!methods.containsKey(methodId)) {
            log.error("Method not found: {}", methodId);
            //response = session.createBytesMessage(serde.writeObject(e));
            //r.fail(1, "Method not found: " + methodId);
          } else {
            Method method = methods.get(methodId);
            Object result = null;
            try {
              if (method.getParameterTypes().length == 0) {
                if (hook != null)
                  hook.serverReceivesCall(methodId, null);
                result = method.invoke(service);
              } else {
                Object[] objects = serde.readObjects(bytesMsg, method.getParameterTypes().length);
                if (hook != null)
                  hook.serverReceivesCall(methodId, objects);
                result = method.invoke(service, objects);
              }
            } catch (InvocationTargetException ex) {
              if (hook != null)
                hook.serverRepliesThrowable("invoking " + methodId, ex.getCause());
              BytesMessage response = serde.writeObject(ex.getCause());
              response.setStringProperty(EXCEPTION, ex.getCause().toString());
              response.setJMSCorrelationID(bytesMsg.getJMSCorrelationID());
              producer.send(bytesMsg.getJMSReplyTo(), response);
            }
            
            if (method.getReturnType().isAssignableFrom(CompletableFuture.class)) {
              ((CompletableFuture<?>) result).whenComplete((reslt, e) -> {
                try{
                  BytesMessage response;
                  if (e == null) {
                    if (hook != null)
                      hook.serverReplies(methodId, reslt);
                    response = serde.writeObject(reslt);
                  } else {
                    if (hook != null)
                      hook.serverRepliesThrowable(methodId, e);
                    response = serde.writeObject(e);
                    response.setStringProperty(EXCEPTION, e.toString());
                  }
                  response.setJMSCorrelationID(bytesMsg.getJMSCorrelationID());
                  producer.send(bytesMsg.getJMSReplyTo(), response);
                }catch(JMSException je){
                  log.error("",je);
                }
              });
            }
          }
        }
      } catch (Throwable e) {
        log.error(e.getMessage(), e);
        //r.fail(-1, e.getMessage());
      }
    };

    try{
      Destination topic = session.createTopic(address + ".topic");
      MessageConsumer topicMsgConsumer = session.createConsumer(topic);
      topicMsgConsumer.setMessageListener(msgListener);
  
      Destination queue = session.createQueue(address + ".queue");
      MessageConsumer queueMsgConsumer = session.createConsumer(queue);
      queueMsgConsumer.setMessageListener(msgListener);
    }catch(JMSException je){
      log.error("",je);
    }

  }

  // Doesn't work with varargs
  static String genMethodId(Method method) {
    StringBuilder sb = new StringBuilder(method.getName());
    sb.append("^");
    for (Class<?> paramT : method.getParameterTypes()) {
      sb.append(paramT.getSimpleName());
    }
    return sb.toString();
  }

  static {
    //log.info("Enabling Kryo TRACE");
    //com.esotericsoftware.minlog.Log.TRACE();
    // To see classes that need registration:
    // grep "Write class name" output.log | cut -d: -f4 | sort | uniq 
  }

  public static interface RpcHook {

    default void clientSendsCall(String methodId, Object[] args) {}

    default void serverReceivesCall(String methodId, Object[] objects) {}

    default void serverReplies(String methodId, Object result) {}

    default void clientReceivesResult(String methodId, Object result) {}

    default void clientReceivedVoid(String methodId) {}

    default void serverRepliesThrowable(String methodId, Throwable e) {}

    default void clientReceivedThrowable(String methodId, Throwable e) {}

    default void clientCallFailed(String methodId, Throwable cause) {}

  }

  @Slf4j
  public static class DebugRpcHook implements RpcHook {
    public void clientSendsCall(String methodId, Object[] args) {
      log.debug("clientSendsCall: {}: {}", methodId, Arrays.toString(args));
    }

    public void serverReceivesCall(String methodId, Object[] args) {
      log.debug("serverReceivesCall: {}: {}", methodId, Arrays.toString(args));
    }

    public void serverReplies(String methodId, Object result) {
      log.debug("serverReplies: {}: {}", methodId, result);
    }

    public void clientReceivesResult(String methodId, Object result) {
      log.debug("clientReceivesResult: {}: {}", methodId, result);
    }

    public void clientReceivedVoid(String methodId) {
      log.debug("clientReceivedVoid: {}", methodId);
    }

    public void serverRepliesThrowable(String methodId, Throwable e) {
      log.debug("serverRepliesThrowable: {}: {}", methodId, (e == null) ? e : e.getMessage());
    }

    public void clientReceivedThrowable(String methodId, Throwable e) {
      log.debug("clientReceivedThrowable: {}: {}", methodId, (e == null) ? e : e.getMessage());
    }

    public void clientCallFailed(String methodId, Throwable e) {
      log.debug("clientCallFailed: {}: {}", methodId, (e == null) ? e : e.getMessage());
    }
  }
}
