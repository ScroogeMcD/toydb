package toydb.toydb;

import java.io.IOException;

public class SimpleToyDBMain {

  public static void main(String[] args) throws IOException {
    SimpleToyDB db = new SimpleToyDB("/Users/priyanka/Desktop/MyProjects/toydb/database");
    /*db.put("Test1","Value1");
    db.put("Test2","Value2");
    db.put("Test3","Value3");
    db.put("Test4","Value4");
    db.put("Test5","Value5");
    db.put("Test6","Value6");

    db.put("Test3","Value3A");
    db.put("Test5","Value5A");*/

    System.out.println("Key : Test2, Val: " + db.get("Test2"));
    System.out.println("Key : Test4, Val: " + db.get("Test4"));
    System.out.println("Key : Test6, Val: " + db.get("Test6"));
    System.out.println("Key : Test8, Val: " + db.get("Test8"));
    System.out.println("Key : Test10, Val: " + db.get("Test10"));

    System.out.println("Key : Test9, Val: " + db.get("Test9"));
    System.out.println("Key : Test7, Val: " + db.get("Test7"));
    System.out.println("Key : Test5, Val: " + db.get("Test5"));
    System.out.println("Key : Test1, Val: " + db.get("Test1"));
    System.out.println("Key : Test3, Val: " + db.get("Test3"));
  }
}
