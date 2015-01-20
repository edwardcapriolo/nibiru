package io.teknek.nibiru.router;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.partitioner.Partitioner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Test;

public class TestTokenRouter {

  public static TreeMap<String,String> threeNodeRing(){
    TreeMap<String,String> tokenMap = new TreeMap<>();
    tokenMap.put("c", "id1");
    tokenMap.put("h", "id2");
    tokenMap.put("r", "id3");
    return tokenMap;
  }
  
  @Test
  public void test(){
    TokenRouter token = new TokenRouter();
    Keyspace keyspace = new Keyspace(new Configuration());
    KeyspaceMetaData meta = new KeyspaceMetaData();
    Map<String,Object> props = new HashMap<>();
    props.put(TokenRouter.TOKEN_MAP_KEY, threeNodeRing());
    meta.setProperties(props);
    keyspace.setKeyspaceMetadata(meta);
    Partitioner p = new NaturalPartitioner();
    Assert.assertEquals("c", token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("a")).get(0).getDestinationId());
    Assert.assertEquals("c", token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("aa")).get(0).getDestinationId());
    Assert.assertEquals("h", token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("h")).get(0).getDestinationId());
    Assert.assertEquals("r", token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("i")).get(0).getDestinationId());
    Assert.assertEquals("c", token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("z")).get(0).getDestinationId());
  }
  
  private ClusterMembership threeLiveNodes(){
    ClusterMembership mock = new ClusterMembership(null, null) {
      public void init() {}
      public void shutdown() {}
      public List<ClusterMember> getLiveMembers() {
       return Arrays.asList( new ClusterMember("127.0.0.1", 2000, 1, "id1"), 
               new ClusterMember("127.0.0.2", 2000, 1, "id2"),
               new ClusterMember("127.0.0.3", 2000, 1, "id3"));
      }
      public List<ClusterMember> getDeadMembers() { return null; }};
      return mock;
  }
  
  @Test
  public void testWithReplicationFactor(){
    TokenRouter token = new TokenRouter();
    Keyspace keyspace = new Keyspace(new Configuration());
    KeyspaceMetaData meta = new KeyspaceMetaData();
    Map<String,Object> props = new HashMap<>();
    props.put(TokenRouter.TOKEN_MAP_KEY, threeNodeRing());
    props.put(TokenRouter.REPLICATION_FACTOR, 2);
    meta.setProperties(props);
    keyspace.setKeyspaceMetadata(meta);
    Partitioner p = new NaturalPartitioner();
    Assert.assertEquals(Arrays.asList(new Destination("c"), new Destination("h")),
            token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("a")));
    Assert.assertEquals(Arrays.asList(new Destination("c"), new Destination("h")),
            token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("aa")));
    Assert.assertEquals(Arrays.asList(new Destination("h"), new Destination("r")),
            token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("h")));
    Assert.assertEquals(Arrays.asList(new Destination("r"), new Destination("c")),
            token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("i")));
    Assert.assertEquals(Arrays.asList(new Destination("c"), new Destination("h")),
            token.routesTo(null, null, keyspace, threeLiveNodes(), p.partition("z")));
  }
  
}
