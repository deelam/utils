package net.deelam.activemq.rpc;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Session;
import org.objenesis.instantiator.ObjectInstantiator;
import org.objenesis.strategy.BaseInstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class KryoSerDe {
  @NonNull
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
      if(bytesMsg.propertyExists(ENCODING_PARAM) && 
          bytesMsg.getIntProperty(ENCODING_PARAM)==JSON_ENCODING) {
        String json = (String) kryo.readClassAndObject(new Input(byteArr));
        Class<?> clazz=Class.forName(bytesMsg.getStringProperty(CLASSNAME_PARAM));
        return (Object[]) getObjectMapper().readValue(json, clazz);
      } else {
        final Input input = new Input(byteArr);
        Object[] result = new Object[count];
        for (int i = 0; i < count; i++)
          result[i] = kryo.readClassAndObject(input);
        return result;
      }
    } catch (Throwable t) {
      log.error("Couldn't read buffer", t);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public synchronized <T> T readObject(BytesMessage bytesMsg) {
    try {
      byte[] byteArr = deserializeByteArray(bytesMsg);
      Object obj = kryo.readClassAndObject(new Input(byteArr));
      if(bytesMsg.propertyExists(ENCODING_PARAM) && 
          bytesMsg.getIntProperty(ENCODING_PARAM)==JSON_ENCODING) {
        Class<?> clazz=Class.forName(bytesMsg.getStringProperty(CLASSNAME_PARAM));
        obj=getObjectMapper().readValue((String) obj, clazz);
      }
      return (T) obj;
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
      if(objs==null || objs.length==0) {
        msg.writeBytes(new byte[0]);
      } else {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        for (int i = 0; i < objs.length; i++)
          kryo.writeClassAndObject(output, objs[i]);
        output.flush();
        msg.writeBytes(baos.toByteArray());
      }
      return msg;
    } catch (Throwable t) {
      try {
        log.warn("Couldn't serialize using Kryo; falling back to Json", t);
        msg.setIntProperty(ENCODING_PARAM, JSON_ENCODING);
        msg.setStringProperty(CLASSNAME_PARAM, objs.getClass().getName());
        return writeObject(getObjectMapper().writeValueAsString(objs));
      } catch (Throwable t2) {
        String arrayStr = Arrays.toString(objs);
        log.error("Couldn't write object of type={}; serializing as string instead: {}", objs.getClass(), arrayStr);
        final Output output = new Output(new ByteArrayOutputStream());
        kryo.writeClassAndObject(output, arrayStr);
        msg.writeBytes(output.toBytes());
        return msg;
      }
    }
  }
  
  public static final String CLASSNAME_PARAM="CLASS";
  public static final String ENCODING_PARAM="ENCODING";
  public static final int JSON_ENCODING=1;
  
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
        log.warn("Couldn't serialize using Kryo; falling back to Json", t);
        msg.setIntProperty(ENCODING_PARAM, JSON_ENCODING);
        msg.setStringProperty(CLASSNAME_PARAM, obj.getClass().getName());
        return writeObject(getObjectMapper().writeValueAsString(obj));
      } catch (Throwable t2) {
        // TODO: 3: determine appropriate kryo serializer
        //log.error("t2",t2);
        String objStr = obj.toString();
        log.error("Couldn't write object of {}; serializing as string instead: {}", obj.getClass(), objStr, t2);
        return writeObject(objStr);
      }
    }
  }

  @Getter(lazy=true)
  private final ObjectMapper objectMapper=privateCreateObjectMapper();
  private ObjectMapper privateCreateObjectMapper() {
    return new ObjectMapper();
  }
}