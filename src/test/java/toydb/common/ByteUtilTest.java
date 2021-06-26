package toydb.common;

import org.junit.Test;

public class ByteUtilTest {

  @Test
  public void hexTobytesTest() {
    System.out.println(ByteUtil.bytesToHex(ByteUtil.hexToBytes("6c")));
  }
}
