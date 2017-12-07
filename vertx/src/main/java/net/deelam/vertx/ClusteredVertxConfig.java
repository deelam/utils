package net.deelam.vertx;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Accessors(fluent=true)
@RequiredArgsConstructor
@Data
@ToString
@Slf4j
public class ClusteredVertxConfig{
  
  public enum CLUSTER_MANAGER { IGNITE_ALL_SERVERS }

  public CLUSTER_MANAGER clusterMgr=CLUSTER_MANAGER.IGNITE_ALL_SERVERS;

  String myIp;
//  String serverIp;
  int serverPort=54321;
  int portRangeSize=100; // must be large enough to support the number of Vertx instances
  String ipPrefix;

  // new props
  String[] clusterIps;
  List<String> clusterIpList;
  int tcpCommPort=49100;
  
  public ClusteredVertxConfig clusterIps(String ipList){
    clusterIps = ipList.split(",");
    return this;
  }
  
  String getMyIp(){
    if(myIp==null){
      myIp = ClusteredVertxConfig.guessMyIPv4Address(ipPrefix); // find an IP address in the same subnet as serverIp
      if(myIp==null && ipPrefix!=null)
        myIp = ClusteredVertxConfig.guessMyIPv4Address(null);
      if(myIp==null)
        log.warn("Could not determine my IP");
    }
    return myIp;
  }
  
  public ClusteredVertxConfig infer(){
    // set defaults
    if(serverPort<=0)
      serverPort=54321;
    if(portRangeSize==0)
      portRangeSize=100;
      
    getMyIp();
    
    if(myIp!=null){
      ipPrefix = myIp.substring(0, myIp.lastIndexOf(".")+1);
      if(clusterIps==null)
        clusterIps=new String[]{ myIp };
    }
    
    // construct IP addresses
    if(clusterIps!=null){
      clusterIpList=new ArrayList<>(clusterIps.length);
      for( int i=0; i<clusterIps.length; i++) {
        clusterIpList.add(clusterIps[i] + ":" + serverPort + ".." + getEndServerPort()); 
      }
    }
    
    return this;
  }

  public int getEndServerPort() {
    return Math.min(serverPort+portRangeSize, 65535);
  }

  public static String guessMyIPv4Address(String ipPrefix) {
    try {
      for (Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces(); e.hasMoreElements();) {
        for (Enumeration<InetAddress> ee = e.nextElement().getInetAddresses(); ee.hasMoreElements();) {
          InetAddress i = (InetAddress) ee.nextElement();
          if (i instanceof Inet4Address) { // I only know how to deal with IPv4 
            int lastOctet = i.getAddress()[3] & 0xFF; // convert to unsigned int
            log.debug("ip={} [3]={}", i, lastOctet);
            if (lastOctet > 1) { // if last byte is 0 or 1, don't choose it
              if(ipPrefix==null || i.getHostAddress().startsWith(ipPrefix)){
                //log.debug("returning ip={}", i);
                return i.getHostAddress();
              }
            }
          }
        }
      }
      log.warn("Cannot determine IP address using: {}", NetworkInterface.getNetworkInterfaces());
    } catch (SocketException se) {
      log.info("Ignoring exception: ", se);
    }
    return null;
  }


  public static void main(String[] args) {
    String myIP = guessMyIPv4Address(null);
    System.out.println(myIP);
  }
}