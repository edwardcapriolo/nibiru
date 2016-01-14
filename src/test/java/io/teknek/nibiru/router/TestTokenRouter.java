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
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestTokenRouter {

  private TokenRouter token;
  private Partitioner partitioner;
  private Keyspace keyspace;
  
  public static TreeMap<String,String> threeNodeRing(){
    TreeMap<String,String> tokenMap = new TreeMap<>();
    tokenMap.put("c", "id1");
    tokenMap.put("h", "id2");
    tokenMap.put("r", "id3");
    return tokenMap;
  }
  
  private static ClusterMembership threeLiveNodes(){
    ClusterMembership mock = new ClusterMembership(null, null) {
      public void init() {}
      public void shutdown() {}
      public List<ClusterMember> getLiveMembers() {
       return Arrays.asList( 
               new ClusterMember("127.0.0.1", 2000, 1, "id1"), 
               new ClusterMember("127.0.0.2", 2000, 1, "id2"),
               new ClusterMember("127.0.0.3", 2000, 1, "id3"));
      }
      public List<ClusterMember> getDeadMembers() { return null; }
    };
    return mock;
  }
  
  public void prepareRouter(int replicationFactor, 
          SortedMap<String,String> tokenMap){
    token = new TokenRouter();
    keyspace = new Keyspace(new Configuration());
    KeyspaceMetaData meta = new KeyspaceMetaData();
    Map<String,Object> props = new HashMap<>();
    props.put(TokenRouter.TOKEN_MAP_KEY, tokenMap);
    props.put(TokenRouter.REPLICATION_FACTOR, replicationFactor);
    meta.setProperties(props);
    keyspace.setKeyspaceMetadata(meta);
    partitioner = new NaturalPartitioner();
  }
  
  private List<Destination> list(String ... destinations){
    List<Destination> results = new java.util.ArrayList<>();
    for (String dest: destinations){
      results.add(new Destination(dest));
    }
    return results;
  }
 
  private List<Destination> locate(String row){
    return token.routesTo(null, keyspace, threeLiveNodes(), 
            partitioner.partition(row));
  }
  
  @Test
  public void testReplication1(){
    prepareRouter(1, threeNodeRing());
    assertEquals(list("id1"), locate("a"));
    assertEquals(list("id1"), locate("aa"));
    assertEquals(list("id2"), locate("h"));
    assertEquals(list("id3"), locate("i"));
    assertEquals(list("id1"), locate("z"));
  }
    
  @Test
  public void testReplication2(){
    prepareRouter(2, threeNodeRing());
    assertEquals(list("id1", "id2"), locate("a"));
    assertEquals(list("id1", "id2"), locate("aa"));
    assertEquals(list("id2", "id3"), locate("h"));
    assertEquals(list("id3", "id1"), locate("i"));
    assertEquals(list("id1", "id2"), locate("z"));
  }
  
}