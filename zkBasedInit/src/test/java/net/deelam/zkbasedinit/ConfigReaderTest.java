package net.deelam.zkbasedinit;

import static net.deelam.zkbasedinit.ConfigReader.CONFIG_ID;
import static net.deelam.zkbasedinit.ConfigReader.getSubconfigs;
import static net.deelam.zkbasedinit.ConfigReader.parseFile;
import static org.junit.Assert.assertEquals;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ImmutableHierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Before;
import org.junit.Test;
import lombok.extern.slf4j.Slf4j;
import net.deelam.zkbasedinit.ConfigReader;

@Slf4j
public class ConfigReaderTest {

  Configuration config;

  @Before
  public void before() throws ConfigurationException {
    config = parseFile("startup.props");
    // toStringConfig(config, config.getKeys());
  }

  @Test
  public void testChildrenOfRoot() {
    assertEquals(ConfigReaderTest.getChildrenOfRoot(config), getChildrenOfRoot(config));
  }

  public static List<String> getChildrenOfRoot(Configuration config) {
    HierarchicalConfiguration<?> hConfig = ConfigurationUtils.convertToHierarchical(config);
    List<ImmutableHierarchicalConfiguration> compIdsC = hConfig.immutableChildConfigurationsAt("");
    // log.info("---------compIdsC--- {}", compIdsC);
    List<String> compIds =
        compIdsC.stream().map(hc -> hc.getRootElementName()).collect(Collectors.toList());
    log.info("---------compIds--- {}", compIds);
    return compIds;
  }

  @Test
  public void testExtractSubconfigs() throws ConfigurationException {
    Map<String, Configuration> subConfigs = ConfigReader.extractSubconfigMap(config);
    // log.info("subConfigs={}", subConfigs);

    getSubconfigs(config).forEach(configMap -> {
      // log.info("{}:\n {}\n {}", configMap.get(CONFIG_ID),
      // subConfigs.get(configMap.get(CONFIG_ID)), configMap);
      Configuration config1 = subConfigs.get(configMap.getString(CONFIG_ID));
      assertEquals(config1.size(), configMap.size());
      configMap.getKeys().forEachRemaining(key->{
        assertEquals(configMap.getProperty(key), config1.getProperty(key));
      });
    });
  }

}
