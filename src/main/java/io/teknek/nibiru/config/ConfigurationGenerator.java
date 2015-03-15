package io.teknek.nibiru.config;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.cluster.GossipClusterMembership;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigurationGenerator {

  public static void main (String [] args) throws FileNotFoundException {
    Configuration configuration = new Configuration();
    configuration.setDataDirectory("/tmp/data");
    configuration.setCommitlogDirectory("/tmp/commit");
    configuration.setTransportHost("127.0.0.1");
    Map<String, Object> clusterProperties = new HashMap<>();
    configuration.setClusterMembershipProperties(clusterProperties);
    List<String> l = new ArrayList();
    l.add("127.0.0.1");
    clusterProperties.put(GossipClusterMembership.HOSTS, l);
    File f = new File ("/tmp/nibiru.xml");
    java.beans.XMLEncoder e = new java.beans.XMLEncoder(new FileOutputStream(f));
    e.writeObject(configuration);
    e.close();
  }
}
