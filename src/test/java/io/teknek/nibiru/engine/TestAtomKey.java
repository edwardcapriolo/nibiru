package io.teknek.nibiru.engine;

import java.util.Arrays;
import java.util.TreeSet;

import org.junit.Test;

import junit.framework.Assert;

public class TestAtomKey {

  @Test
  public void orderTest(){
    TreeSet<AtomKey> list = new TreeSet<AtomKey>();
    list.addAll(Arrays.asList(new ColumnKey("ab"), new ColumnKey("ac"), new RowTombstoneKey(), 
            new RangeTombstoneKey("d", "f")));
    Assert.assertEquals(RowTombstoneKey.class, list.first().getClass());
    Assert.assertEquals(RangeTombstoneKey.class, list.higher(list.first()).getClass());
    Assert.assertEquals(new ColumnKey("ac").getColumn() , ((ColumnKey)list.last() ).getColumn());
  }
}
