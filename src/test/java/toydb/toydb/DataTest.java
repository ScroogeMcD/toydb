package toydb.toydb;

import org.junit.Test;

public class DataTest {

  @Test
  public void testSerializationDeserialization() {
    // Data data = new Data("Test","This is a very long value that I am gene00rating { so do not
    // blame me !@#$%^&*()_+}");
    Data data = new Data("Key10", "Value10");
    byte[] byteArr = data.serialize();
    Data deserializedData = new Data(byteArr);
    System.out.println(deserializedData);
  }
}
