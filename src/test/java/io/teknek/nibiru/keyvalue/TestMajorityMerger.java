package io.teknek.nibiru.keyvalue;

import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.Assert;
import io.teknek.nibiru.coordinator.MajorityValueResultMerger;
import io.teknek.nibiru.transport.Response;

import org.junit.Test;

public class TestMajorityMerger {

  @Test
  public void majority(){
    MajorityValueResultMerger mm = new MajorityValueResultMerger();
    Assert.assertEquals("a", mm.merge(Arrays.asList( 
            new Response().withProperty("payload", "a"),
            new Response().withProperty("payload", "b"),
            new Response().withProperty("payload", "a")
            ), null).get("payload"));    
  }
  
  @Test
  public void anotherMajority(){
    MajorityValueResultMerger mm = new MajorityValueResultMerger();
    Assert.assertEquals("b", mm.merge(Arrays.asList( 
            new Response().withProperty("payload", "a"),
            new Response().withProperty("payload", "b"),
            new Response().withProperty("payload", "b")
            ), null).get("payload"));    
  }
  
  @Test
  public void aPlurality(){
    MajorityValueResultMerger mm = new MajorityValueResultMerger();
    Assert.assertEquals("a", mm.merge(Arrays.asList( 
            new Response().withProperty("payload", "a"),
            new Response().withProperty("payload", "a"),
            new Response().withProperty("payload", "b"),
            new Response().withProperty("payload", "b")
            ), null).get("payload"));    
    
    Assert.assertEquals("a", mm.merge(Arrays.asList( 
            new Response().withProperty("payload", "b"),
            new Response().withProperty("payload", "b"),
            new Response().withProperty("payload", "a"),
            new Response().withProperty("payload", "a")
            ), null).get("payload")); 
    
    Assert.assertEquals(null, mm.merge(new ArrayList<Response>(), null).get("payload"));
  }
 
}
