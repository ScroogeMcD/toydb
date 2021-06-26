package toydb.toydb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import toydb.index.Memtable;

public class SSTableBasedToyDB implements ToyDB {

  private List<Memtable> memTables;
  private String dbDirectoryPath;

  private static final Integer SSTABLE_MAX_SIZE_IN_BYTES = 2 * 1024 * 1024; // 2 MB

  public SSTableBasedToyDB(String dbDirectoryPath) throws IOException {
    this.dbDirectoryPath = dbDirectoryPath;
    initDB();
  }

  private void initDB() throws IOException {
    // STEP 01 : read all the serialized files from disk and create the SPARSE memTables list
    memTables = new ArrayList<>(); // TODO : read from disk

    // STEP 02 : read from the writeAheadLog and create the current DENSE memtable
    memTables.add(0, new Memtable(dbDirectoryPath, "sortedStringFile_" + memTables.size()));
  }

  @Override
  public void put(String key, String value) {
    Memtable currMemTable = memTables.get(0);

    if (currMemTable.getSizeInBytes() + key.length() + value.length() > SSTABLE_MAX_SIZE_IN_BYTES) {
      try {
        // System.out.println("Serializing current memtable to file on disk.");
        // this can be done in the background
        currMemTable.persistOnDisk();
        // System.out.println("Creating a new memtable");
        currMemTable = new Memtable(dbDirectoryPath, "sortedStringFile_" + memTables.size());
        memTables.add(0, currMemTable);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    try {
      currMemTable.put(key, value);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public String get(String key) {
    String value = null;

    try {
      for (Memtable memtable : memTables) {
        String val = memtable.get(key);
        if (val != null) {
          value = val;
          break;
        }
      }
    } catch (Exception e) {
      System.out.print("Exception encountered while searching for key : " + key + " | " + e);
    }

    return value;
  }

  @Override
  public void delete(String key) {}
}
