package net.deelam.vertx.rpc;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import org.objenesis.instantiator.ObjectInstantiator;
import org.objenesis.strategy.BaseInstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.Json;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Based on https://github.com/xored/vertx-typed-rpc
 */
@Slf4j
@RequiredArgsConstructor
public class VertxRpcUtil {
  final Vertx vertx;
  final String address;

  @Setter
  RpcHook hook;

  private static final String HEADER_METHOD_ID = "method";
  private static final String EXCEPTION = "exception";


  @SuppressWarnings("unchecked")
  public <T> T createClient(Class<T> iface) {
    log.debug("Creating RPC client for {} at {} ", iface.getSimpleName(), address);
    final HashMap<Method, String> methods = new HashMap<>();
    final KryoSerDe serde = new KryoSerDe(address + "-client");
    final Context context = vertx.getOrCreateContext();
    return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[] {iface},
        (proxy, method, args) -> {
          if ("toString".equals(method.getName())) {
            return (T) "RPC client proxy for " + iface.getSimpleName();
          }
          try {
            String methodIdTemp = methods.get(method);
            if (methodIdTemp == null) {
              method.setAccessible(true);
              methodIdTemp = genMethodId(method);
              methods.put(method, methodIdTemp);
            }
            final String methodId = methodIdTemp;
            if (hook != null)
              hook.clientSendsCall(methodId, args);
            final DeliveryOptions options = new DeliveryOptions().addHeader(HEADER_METHOD_ID, methodId);
            final Buffer buffer = (method.getParameterTypes().length > 0) ? serde.writeObjects(args) : null;
            final Class<?> returnType = method.getReturnType();
            if (returnType == void.class) {
              final CompletableFuture<Object> resultF = new CompletableFuture<>();
              context.runOnContext(v -> {
                if (method.getName().startsWith("publish"))
                  vertx.eventBus().publish(address, buffer, options);
                else
                  vertx.eventBus().send(address, buffer, options, resultHandler(serde, methodId, resultF));
              });
              return resultF;
            } else if (returnType.isAssignableFrom(CompletableFuture.class)) {
              final CompletableFuture<Object> resultF = new CompletableFuture<>();
              context.runOnContext(v -> {
                vertx.eventBus().<Buffer>send(address, buffer, options, resultHandler(serde, methodId, resultF));
              });
              return resultF;
            } else {
              throw new UnsupportedOperationException(
                  "Unsupported method '" + method + "' with return type=" + returnType);
            }
          } catch (Throwable e) {
            log.error("RPC client-side error", e);
            return null;
          }
        });
  }

  private Handler<AsyncResult<Message<Buffer>>> resultHandler(final KryoSerDe serde, final String methodId,
      final CompletableFuture<Object> resultF) {
    return r -> {
      if (r.failed()) {
        if (hook != null)
          hook.clientCallFailed(methodId, r.cause());
        resultF.completeExceptionally(r.cause());
      } else {
        Message<Buffer> msg = r.result();
        if (msg == null) {
          if (hook != null)
            hook.clientReceivedVoid(methodId);
          resultF.complete(null);
        } else {
          String exceptionStr = r.result().headers().get(EXCEPTION);
          try {
            Object result = serde.readObject(msg.body());
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
          } catch (Throwable e) {
            log.error("RPC client-side error: cannot read msg body for {}:", methodId, e);
            resultF.completeExceptionally(new RuntimeException("Cannot read msg body for " + methodId));
          }
        }
      }
    };
  }

  public <T> CompletableFuture<MessageConsumer<Buffer>> registerServer(T service) {
    log.debug("Registering RPC server at {}: {}", address, service.getClass().getName());
    HashMap<String, Method> methods = new HashMap<>();
    for (Method method : service.getClass().getDeclaredMethods()) {
      method.setAccessible(true);
      methods.put(genMethodId(method), method);
    }
    CompletableFuture<MessageConsumer<Buffer>> msgConsumerF = new CompletableFuture<>();
    //new Thread(()->{
    final KryoSerDe serde = new KryoSerDe(address + "-service");
//    final Context context = vertx.getOrCreateContext();
//    context.runOnContext(v->{
    MessageConsumer<Buffer> msgConsumer = vertx.eventBus().<Buffer>consumer(address, r -> {
      try {
        String methodId = r.headers().get(HEADER_METHOD_ID);
        if (!methods.containsKey(methodId)) {
          log.error("RPC method not found for {} at {}: {}", service.getClass(), address, methodId);
          r.fail(1, "Method not found for " + service.getClass() + ": " + methodId);
        } else {
          Method method = methods.get(methodId);
          Object result = null;
          try {
            if (method.getParameterTypes().length == 0) {
              if (hook != null)
                hook.serverReceivesCall(methodId, null);
              result = method.invoke(service);
            } else {
              Object[] args = serde.readObjects(r.body(), method.getParameterTypes().length);
              if (hook != null)
                hook.serverReceivesCall(methodId, args);
              result = method.invoke(service, args);
            }

            if (method.getReturnType().isAssignableFrom(CompletableFuture.class)) {
              ((CompletableFuture<?>) result).whenComplete((reslt, e) -> {
                if (e == null) {
                  if (hook != null)
                    hook.serverReplies(methodId, reslt);
                  r.reply(serde.writeObject(reslt));
                } else {
                  if (hook != null)
                    hook.serverRepliesThrowable(methodId, e);
                  log.error("RPC server-side error running method: {}", methodId, e);
                  DeliveryOptions options = new DeliveryOptions().addHeader(EXCEPTION, e.toString());
                  r.reply(serde.writeObject(e), options);
                }
              });
            } else if (result==null){
              if (method.getName().startsWith("publish")){
                // don't reply
              } else {
                r.reply(serde.writeObject(null));
              }
            } else {
              log.error("?What to do with non-CompletableFuture result: {}", result);
            }
          } catch (InvocationTargetException ex) {
            Throwable e = (ex.getCause() == null) ? ex : ex.getCause();
            //checkNotNull(e, ex.getCause());
            if (hook != null)
              hook.serverRepliesThrowable("invoking " + methodId, e);
            log.error("RPC server-side error: {}", methodId, e);
            DeliveryOptions options = new DeliveryOptions().addHeader(EXCEPTION, e.toString());
            r.reply(serde.writeObject(e), options); //r.reply(serde.writeObject(ex.getTargetException().getMessage()));
          } catch (IllegalArgumentException | IllegalAccessException e) {
            if (hook != null)
              hook.serverRepliesThrowable("invoking " + methodId, e);
            log.error("RPC server-side method call error: {}", methodId, e);
            DeliveryOptions options = new DeliveryOptions().addHeader(EXCEPTION, e.toString());
            r.reply(serde.writeObject(e), options); //r.reply(serde.writeObject(ex.getTargetException().getMessage()));
          }
        }
      } catch (Throwable e) {
        log.error("RPC unexpected: " + e.getMessage(), e);
        r.fail(-1, e.getMessage());
      }
    });
    msgConsumerF.complete(msgConsumer);
//    });
    return msgConsumerF;
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
    //com.esotericsoftware.minlog.Log.TRACE();
  }

  /**
   * Reminder: Do NOT use varargs as parameters to RPC methods.
   * Doing so causes Kyro to fail, due to wrong parameter count, due to wrong method selection.
   */
  public static final class KryoSerDe {
    Kryo kryo;
    final String name;

    public static Map<Class<?>, Integer> classRegis;

    public KryoSerDe(String name) {
      this.name = name;
      kryo = newKryo();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Kryo newKryo() {
      Kryo kryo = new Kryo();
      if (classRegis != null) {
        kryo.setRegistrationRequired(true);
        for (Entry<Class<?>, Integer> e : classRegis.entrySet()) {
          log.info("Registering {}={}", e.getKey(), e.getValue());
          kryo.register(e.getKey(), e.getValue());
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
              if (type.isInterface())
                if (List.class.isAssignableFrom(type)) {
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
      log.trace("RPC Created new Kryo for {}: {}", name, kryo);
      return kryo;
    }

    public synchronized Registration readClass(Input input) {
      try {
        //log.debug("Using kryo={} to read for {}", kryo, name);
        return kryo.readClass(input);
      } catch (Throwable t) {
        log.error("RPC Couldn't read input", t);
        throw t; //return null;
      }
    }

    public synchronized Object[] readObjects(Buffer buffer, int count) {
      try {
        //log.debug("Using kryo={} to read for {}", kryo, name);
        final Input input = new Input(buffer.getBytes());
        Object[] result = new Object[count];
        for (int i = 0; i < count; i++)
          result[i] = kryo.readClassAndObject(input);
        return result;
      } catch (Throwable t) {
        log.error("RPC Couldn't read buffer", t);
        throw t;
      }
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> T readObject(Buffer buffer) {
      try {
        //log.debug("Using kryo={} to read for {}", kryo, name);
        return (T) kryo.readClassAndObject(new Input(buffer.getBytes()));
      } catch (Throwable t) {
        log.error("RPC Couldn't read buffer", t);
        throw t; //return null;
      }
    }

    public synchronized Buffer writeObjects(Object[] objs) {
      try {
        //log.debug("Using kryo={} to write for {}", kryo, name);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        for (int i = 0; i < objs.length; i++)
          kryo.writeClassAndObject(output, objs[i]);
        output.flush();
        return Buffer.buffer(baos.toByteArray());
      } catch (Throwable t) {
        log.error("t", t);
        String arrayStr = Arrays.toString(objs);
        log.error("RPC Couldn't write object of type={}; serializing as string instead: {}", objs.getClass(), arrayStr);
        final Output output = new Output(new ByteArrayOutputStream());
        kryo.writeClassAndObject(output, arrayStr);
        return Buffer.buffer(output.toBytes());
      }
    }

    public synchronized Buffer writeObject(Object obj) {
      try {
        //log.debug("Using kryo={} to write for {}", kryo, name);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        //log.debug("writeObject: " + obj);
        kryo.writeClassAndObject(output, obj);

        //        Object debugCheck=kryo.readClassAndObject(new Input(output.toBytes()));
        //        log.debug("debugCheck: " + debugCheck);

        output.flush();
        return Buffer.buffer(baos.toByteArray());
      } catch (Throwable t) {
        try {
          log.error("t", t);
          return writeObject(Json.encode(obj));
        } catch (Throwable t2) {
          // TODO: 6: determine appropriate kryo serializer
          log.error("t2", t2);
          String objStr = obj.toString();
          log.error("RPC Couldn't write object of {}; serializing as string instead: {}", obj.getClass(), objStr);
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
}
