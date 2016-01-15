package io.teknek.nibiru.engine;

import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.engine.atom.TombstoneValue;

import org.junit.Assert;
import org.junit.Test;

public class TestDefaultColumnFamily {

  @Test
  public void testTwoColumns() {
    ColumnValue cvOld = new ColumnValue();
    cvOld.setTime(2);
    ColumnValue cvNew = new ColumnValue();
    cvNew.setTime(3);
    Assert.assertEquals(cvNew, DefaultColumnFamily.applyRules(cvOld, cvNew));
    Assert.assertEquals(cvNew, DefaultColumnFamily.applyRules(cvNew, cvOld));
  }
  
  @Test
  public void testTombstoneShadow() {
    ColumnValue cvOld = new ColumnValue();
    cvOld.setTime(3);
    TombstoneValue cvNew = new TombstoneValue(3);
    Assert.assertEquals(cvNew, DefaultColumnFamily.applyRules(cvOld, cvNew));
    Assert.assertEquals(cvNew, DefaultColumnFamily.applyRules(cvNew, cvOld));
    ColumnValue cvNewer = new ColumnValue();
    cvNewer.setTime(4);
    Assert.assertEquals(cvNewer, DefaultColumnFamily.applyRules(cvOld, cvNewer));
    Assert.assertEquals(cvNewer, DefaultColumnFamily.applyRules(cvNewer, cvOld));
  }
 
}
