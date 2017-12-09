package net.deelam.activemq;

import java.util.Arrays;

public final class ConstantsAmq {

  public static final String BROKER_NAME = "BROKER_NAME";
  public static final String BROKER_URL = "BROKER_URL"; // convenience for BROKER_URLS[0]
  public static final String BROKER_URLS = "BROKER_URLS";

  public static String[] parseBrokerUrls(String brokerUrls) {
    return Arrays.stream(brokerUrls.split(",")).map(String::trim).toArray(String[]::new);
  }

  public static String getTcpBrokerUrl(String brokerUrls) {
    return Arrays.stream(brokerUrls.split(",")).map(String::trim).filter(s -> s.toLowerCase().startsWith("tcp:"))
        .findFirst().get();
    //    throw new IllegalArgumentException("Could not identify URL starting with 'tcp://'");

  }
}
