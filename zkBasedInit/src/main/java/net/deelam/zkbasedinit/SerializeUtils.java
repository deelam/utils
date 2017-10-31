package net.deelam.zkbasedinit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;

public final class SerializeUtils {

  private SerializeUtils() {}

  public static byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream os = new ObjectOutputStream(out);
    os.writeObject(obj);
    return out.toByteArray();
  }

  public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    ObjectInputStream is = new ObjectInputStream(in);
    return is.readObject();
  }

  public static byte[] serialize(Configuration config) throws IOException {
    Properties props = ConfigurationConverter.getProperties(config);
    return serialize(props);
  }

  public static Properties deserializeConfigurationAsProperties(byte[] data)
      throws IOException, ClassNotFoundException {
    return (Properties) deserialize(data);
  }
  
  public static Map<Object, Object> deserializeConfigurationAsMap(byte[] data)
      throws IOException, ClassNotFoundException {
    Properties props = (Properties) deserialize(data);
    return ConfigurationConverter.getMap(ConfigurationConverter.getConfiguration(props));
  }
}
