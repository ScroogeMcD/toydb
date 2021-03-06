package toydb.toydb;

import java.io.IOException;
import toydb.datafile.DataFile;
import toydb.datafile.DataFileIterator;
import toydb.index.DBIndex;
import toydb.index.HashBasedIndex;

/**
 * SimpleToyDB uses one file to persist data.
 *
 * <p>Persistence: The <Key, Value> pairs are written sequentially to the file.
 *
 * <p>Index : An in memory hash based index is used. This index stores a Key to corresponding latest
 * entry location in the file.
 *
 * <p>Read : In memory index is used to find the location of the key in the file. Then the file is
 * read that location to get the corresponding value.
 *
 * <p>Delete : Key is stored with a tombstone in the file.
 *
 * <p>Limitations : - All keys need to be present in the memory. - No range search based queries
 * possible.
 */
public class SimpleToyDB implements ToyDB {

  private String dbBasePath;

  private DataFile dataFile;

  private DBIndex index;

  public SimpleToyDB(String dbPath) throws IOException {
    this.dbBasePath = dbPath;
    dataFile = new DataFile(dbBasePath + "/toydb_datafile_1");
    index = new HashBasedIndex();
    initDB();
  }

  private void initDB() {
    // Read all data in dataFile and initialize the hashIndex
    DataFileIterator it = new DataFileIterator(dataFile);
    while (it.hasNext()) {
      long location = it.getReadLocation();
      Data d = it.next();
      index.updateIndex(d.getKey(), location);
    }
  }

  @Override
  public void put(String key, String value) {
    try {
      Data data = new Data(key, value);
      long location = dataFile.write(data);
      index.updateIndex(key, location);
    } catch (Exception e) {
      System.out.println("An exception occurred. The data could not be written. " + e);
    }
  }

  @Override
  public String get(String key) {
    String value = "";
    try {
      Long location = index.getLocationFromIndex(key);
      if (location == null) return "";
      Data data = dataFile.read(location);
      value = data.getValue();
    } catch (Exception e) {
      System.out.println("An exception occurred. The data could not be read. " + e);
    }

    return value;
  }

  @Override
  public void delete(String key) {
    System.out.println("Delete method is not yet implemented.");
  }
}
