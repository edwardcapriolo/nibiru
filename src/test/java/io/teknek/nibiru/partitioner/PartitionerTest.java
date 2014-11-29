package io.teknek.nibiru.partitioner;
import io.teknek.nibiru.partitioner.Md5Partitioner;
import junit.framework.Assert;

import org.junit.Test;

public class PartitionerTest {

  @Test
  public void test(){
    Md5Partitioner p = new Md5Partitioner();
    Assert.assertEquals("49f68a5c8493ec2c0bf489821c21fc3b", p.partition("hi").getToken());
  }
}
