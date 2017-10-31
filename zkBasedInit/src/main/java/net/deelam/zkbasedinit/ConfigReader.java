package net.deelam.zkbasedinit;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigReader {

  public static final String CONFIG_ID = "CONFIG_ID";

  public static void main(String[] args) throws ConfigurationException {
    Configuration config = parseFile("startup.props");
    log.info("{}\n------", toStringConfig(config, config.getKeys()));

    Stream<Configuration> subConfigsS = getSubconfigs(config);
    subConfigsS.forEach(configMap -> log.info("{}: {}", configMap.getString(CONFIG_ID), configMap));
  }

  public static Map<String, Configuration> extractSubconfigMap(Configuration config) {
    return getChildrenOfRoot(config).stream().map(compId -> {
      Configuration configMap = extractSubconfig(config, compId);
      // log.info("{}: \n {}", compId, configMap.toString().replaceAll(", ", "\n "));
      return Pair.of(compId, configMap);
    }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  public static Stream<Configuration> getSubconfigs(Configuration config) {
    return getChildrenOfRoot(config).stream().map(compId -> extractSubconfig(config, compId));
  }

  public static Set<String> getChildrenOfRoot(Configuration config) {
    Set<String> topChildren = new HashSet<>();
    config.getKeys().forEachRemaining(k -> topChildren.add(k.substring(0, k.indexOf('.'))));
    return topChildren;
  }

  private static boolean includeInclude = true;

  public static Configuration extractSubconfig(Configuration config, String prefix) {
    Configuration subConfigMap = new BaseConfiguration();
    subConfigMap.setProperty(CONFIG_ID, prefix);

    String includeKey = prefix + ".include";
    config.getKeys(prefix).forEachRemaining(k -> {
      if (k.equals(includeKey)) {
        String includePrefixes = config.getString(k);
        expandIncludeDirective(config, subConfigMap, includePrefixes);
        if (includeInclude)
          addToMap(subConfigMap, k, includePrefixes);
      } else {
        addToMap(subConfigMap, k, config.getProperty(k));
      }
    });
    return subConfigMap;
  }

  public static void expandIncludeDirective(Configuration config, Configuration subConfigMap,
      String includePrefixes) {
    for (String includePrefix : includePrefixes.split(",")) {
      String trimmedPrefix = includePrefix.trim();
      if (trimmedPrefix.endsWith(".include")) {
        String nestedIncludeValue = config.getString(trimmedPrefix);
        expandIncludeDirective(config, subConfigMap, nestedIncludeValue);
      } else {
        config.getKeys(trimmedPrefix).forEachRemaining(
            includeK -> addToMap(subConfigMap, includeK, config.getProperty(includeK)));
      }
    }
  }

  public static boolean addToMap(Configuration subConfigMap, String includeK, Object value) {
    if (value == null)
      return false;
    String keyWithoutFirstTerm = includeK.substring(includeK.indexOf('.') + 1);
    if (subConfigMap.containsKey(keyWithoutFirstTerm)) {
      if (subConfigMap.getProperty(keyWithoutFirstTerm).equals(value))
        return true; // treat duplicate entries as a successful put
      else {
        log.warn("Ignoring duplicate key '{}' with different value: {}", keyWithoutFirstTerm,
            value);
        return false;
      }
    } else {
      subConfigMap.setProperty(keyWithoutFirstTerm, value);
      return true;
    }
  }

  @SuppressWarnings("squid:CommentedOutCodeLine")
  public static String toStringConfig(Configuration config, Iterator<String> iterator) {
    StringWriter w = new StringWriter();
    if (iterator == null) {
      ConfigurationUtils.dump(config, new PrintWriter(w));
    } else {
      iterator.forEachRemaining(k -> w.append(k + "=" + config.getString(k) + ", "));
    }
    return w.toString();
  }

  static Configuration parseFile(String propFilename) throws ConfigurationException {
    Configurations configs = new Configurations();
    return configs.properties(new File(propFilename));
  }

}
