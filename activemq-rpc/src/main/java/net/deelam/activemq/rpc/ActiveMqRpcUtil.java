package net.deelam.activemq.rpc;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

import org.objenesis.instantiator.ObjectInstantiator;
import org.objenesis.strategy.BaseInstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ActiveMqRpcUtil {
  final Session session;
  final String address;
  int deliveryMode = DeliveryMode.NON_PERSISTENT;
  final Map<String, CompletableFuture<Object>> returnObjs = new HashMap<>();

  @Setter
  RpcHook hook;
  private int correlIdCounter;

  private static final String HEADER_METHOD_ID = "method";
  private static final String EXCEPTION = "exception";


  @SuppressWarnings("unchecked")
  public <T> T createClient(Class<T> iface) throws JMSException {
    final KryoSerDe serde = new KryoSerDe(session);

    Destination topic = session.createTopic(address + ".topic");
    MessageProducer topicMsgProducer = session.createProducer(topic);
    topicMsgProducer.setDeliveryMode(deliveryMode);

    Destination queue = session.createQueue(address + ".queue");
    MessageProducer queueMsgProducer = session.createProducer(queue);
    queueMsgProducer.setDeliveryMode(deliveryMode);

    Destination replyQueue = session.createQueue(address + ".replyQ");
    MessageConsumer responseConsumer = session.createConsumer(replyQueue);
    responseConsumer.setMessageListener((msg) -> {
      try {
        String corrId = msg.getJMSCorrelationID();
        CompletableFuture<Object> resultF = returnObjs.get(corrId);

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
      } catch (Exception e) {
        log.error("Caught:", e);
      }

    });

    //    Context context = vertx.getOrCreateContext();
    //    log.info("vertxContext={} for client class={}", context, iface.getSimpleName());
    log.info("client class={}", iface.getSimpleName());
    //    if(context.get("dnlam")!=null)
    //      log.warn("Reusing context with deploymentId={}", context.deploymentID());
    //    context.put("dnlam","dnlam");
    //    checkNotNull(context);
    return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[] {iface},
        (proxy, method, args) -> {
          try {
            String methodId = genMethodId(method);
            final BytesMessage msg = (method.getParameterTypes().length > 0) ? serde.writeObjects(args) : null;
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
              String correlStr = Integer.toString(++correlIdCounter);
              msg.setJMSCorrelationID(correlStr);
              final CompletableFuture<Object> resultF = new CompletableFuture<>();
              returnObjs.put(correlStr, resultF);
              queueMsgProducer.send(msg);
              log.debug("client sent {} with args={}", methodId, Arrays.toString(args));
              return resultF;
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

  public static final class KryoSerDe {
    final Session session;
    Kryo kryo = new Kryo();

    public static Map<Class<?>, Integer> classRegis;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public KryoSerDe(Session session) {
      this.session=session;
      if (classRegis != null) {
        kryo.setRegistrationRequired(true);
        for (Map.Entry<Class<?>, Integer> e : classRegis.entrySet()) {
          int intAssignment = 100 + e.getValue(); // add 100 to ensure no collision with default registrations
          log.info("Registering {}={}", e.getKey(), intAssignment);
          kryo.register(e.getKey(), intAssignment);
        }
      }

      kryo.register(Map.class, new MapSerializer() {
        protected Map create(Kryo kryo, Input input, java.lang.Class<Map> type) {
          return new HashMap();
        }
      });
      ((DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
          .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
      kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(
          new BaseInstantiatorStrategy() {
            @Override
            public ObjectInstantiator newInstantiatorOf(Class type) {
              if (type.isInterface() && List.class.isAssignableFrom(type)) {
                return new ObjectInstantiator<List>() {
                  @Override
                  public List newInstance() {
                    return new ArrayList();
                  }
                };
              }
              return new StdInstantiatorStrategy().newInstantiatorOf(type);
            }
          }));
    }

    public synchronized Registration readClass(Input input) {
      try {
        return kryo.readClass(input);
      } catch (Throwable t) {
        log.error("Couldn't read input", t);
        return null;
      }
    }

    public synchronized Object[] readObjects(BytesMessage bytesMsg, int count) {
      try {
        byte[] byteArr = deserializeByteArray(bytesMsg);
        final Input input = new Input(byteArr);
        Object[] result = new Object[count];
        for (int i = 0; i < count; i++)
          result[i] = kryo.readClassAndObject(input);
        return result;
      } catch (Throwable t) {
        log.error("Couldn't read buffer", t);
        return null;
      }
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> T readObject(BytesMessage bytesMsg) {
      try {
        byte[] byteArr = deserializeByteArray(bytesMsg);
        return (T) kryo.readClassAndObject(new Input(byteArr));
      } catch (Throwable t) {
        log.error("Couldn't read buffer", t);
        return null;
      }
    }

    private byte[] deserializeByteArray(BytesMessage bytesMsg) throws JMSException {
      byte[] byteArr;
      if (bytesMsg.getBodyLength() < Integer.MAX_VALUE) {
        byteArr = new byte[(int) bytesMsg.getBodyLength()];
        bytesMsg.readBytes(byteArr);
      } else {
        log.warn("Untested");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int n = 0;
        byte[] buf = new byte[Integer.MAX_VALUE];
        while ((n = bytesMsg.readBytes(buf)) >= 0) {
          out.write(buf, 0, n);
        }
        byteArr = out.toByteArray();
      }
      return byteArr;
    }

    public synchronized BytesMessage writeObjects(Object[] objs) throws JMSException {
      BytesMessage msg = session.createBytesMessage();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        for (int i = 0; i < objs.length; i++)
          kryo.writeClassAndObject(output, objs[i]);
        output.flush();
        msg.writeBytes(baos.toByteArray());
        return msg;
      } catch (Throwable t) {
        String arrayStr = Arrays.toString(objs);
        log.error("Couldn't write object of type={}; serializing as string instead: {}", objs.getClass(), arrayStr);
        final Output output = new Output(new ByteArrayOutputStream());
        kryo.writeClassAndObject(output, arrayStr);
        msg.writeBytes(output.toBytes());
        return msg;
      }
    }

    public synchronized BytesMessage writeObject(Object obj) throws JMSException {
      BytesMessage msg = session.createBytesMessage();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        log.debug("writeObject: " + obj);
        kryo.writeClassAndObject(output, obj);
        output.flush();
        msg.writeBytes(baos.toByteArray());
        return msg;
      } catch (Throwable t) {
        try {
          //log.error("t",t);
          return writeObject(obj.toString());//FIXME: Json.encode(obj));
        } catch (Throwable t2) {
          // TODO: 3: determine appropriate kryo serializer
          //log.error("t2",t2);
          String objStr = obj.toString();
          log.error("Couldn't write object of {}; serializing as string instead: {}", obj.getClass(), objStr);
          return writeObject(objStr);
        }
      }
    }
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
