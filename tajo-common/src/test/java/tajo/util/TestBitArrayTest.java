package tajo.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBitArrayTest {

  @Test
  public void test() {
    int num = 80;
    BitArray bitArray = new BitArray(num);

    for (int i = 0; i < num; i++) {
      assertFalse(bitArray.get(i));
      bitArray.set(i);
      assertTrue(bitArray.get(i));
    }
  }
}
