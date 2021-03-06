package net.deelam.coordworkers;

import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ComponentConfigI;
import net.deelam.zkbasedinit.ComponentI;
import net.deelam.zkbasedinit.ConstantsZk;

@Slf4j
@Getter
public class AbstractCompConfig implements ComponentConfigI {

  private static final String ZOOKEEPER_CONNECT = ConstantsZk.ZOOKEEPER_CONNECT;
  final String componentId;
  final String zookeeperConnectString;

  // populate and print remaining unused properties
  public AbstractCompConfig(Properties props) {
    componentId = useRequiredProperty(props, ComponentI.COMPONENT_ID);
    zookeeperConnectString = useProperty(props, ZOOKEEPER_CONNECT, null);
    log.debug("Properties for component '{}': {}", componentId, props);
  }

  public String useProperty(Properties props, String propkey, String defaultValue) {
    String val = props.getProperty(propkey, defaultValue);
    props.remove(propkey);
    return val;
  }

  public String useRequiredProperty(Properties props, String propkey) {
    String val = props.getProperty(propkey);
    if (val == null)
      throw new IllegalArgumentException(
          "Missing required config: " + propkey + "  props=" + props);
    props.remove(propkey);
    return val;
  }

  public String useRequiredRefProperty(Properties props, String propkey) {
    String refPath = props.getProperty(propkey);
    if (refPath == null)
      throw new IllegalArgumentException(
          "Missing required config: " + propkey + "  props=" + props);
    props.remove(propkey);

    String resolvedVal = props.getProperty(refPath);
    if (resolvedVal == null)
      throw new IllegalArgumentException(
          "Missing required config: " + refPath + "  props=" + props);
    props.remove(refPath);
    return resolvedVal;
  }

  public void checkRemainingProps(Properties props) {
    props.remove("CONFIG_ID");
    props.remove(ZOOKEEPER_CONNECT);
    props.remove(ComponentI.ZK_PATH);
    props.remove("classname");
    props.remove("include");
    if (props.size() > 0)
      log.warn("Remaining unused properties for {}: {}", componentId, props);
  }
}
