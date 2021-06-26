package toydb.index;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import kotlin.text.Charsets;
import toydb.common.Pair;
import toydb.common.RBTree;
import toydb.datafile.DataFile;
import toydb.datafile.DataFileIterator;
import toydb.toydb.Data;

/**
 * This class represents an in memory table, and its associated on-disk file.
 *
 * <p>The memtable contains an in memory table of key_value pairs, and when the table gets filled
 * up, it is serialized and written on the disk.
 */
public class Memtable {

  private DataFile df;
  private RBTree index;
  private INDEX_TYPE indexType;
  private BloomFilter<String> bloomFilter;
  private File writeAheadLogFile;
  private FileWriter walFW;
  private

  enum INDEX_TYPE {
    DENSE_INDEX,
    SPARSE_INDEX
  }

  public Memtable(String dataFileBasePath, String dataFileName) throws IOException {
    String datafilePath = dataFileBasePath + "/" + dataFileName;
    df = new DataFile(datafilePath);
    index = new RBTree();
    indexType = INDEX_TYPE.DENSE_INDEX;
    writeAheadLogFile = new File(dataFileBasePath + "/write_ahead_log_" + dataFileName + ".txt");
    walFW = new FileWriter(writeAheadLogFile, true);
  }

  public void put(String key, String value) throws IOException {
    /*Files.write(
        writeAheadLogFilePath,
        new Data(key, value).serialize(),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);*/
    walFW.append(key + "\0\0" + value);
    walFW.flush();
    index.insert(key, value);
  }

  public String get(String key) throws IOException {
    // STEP 01 : If it is DENSE_INDEX, then check for key,value in memory
    if (indexType == INDEX_TYPE.DENSE_INDEX) return index.get(key);
    /*
       STEP 02 : It is a SPARSE INDEX
       - First check if this key is present in the sparse index itself. If yes, then get the location pointer from memory,
         and read the value at this location.
       - If key not present in sparse index, get the predecessor and successor location pointers for this key, from sparse index.
       - Iterate between this predecessor and successor on the disk, and see if this key is found. If found, return value.
       - If not, return null.
    */
    else {
      // If not present in BloomFilter return null
      if (!bloomFilter.mightContain(key)) return null;

      DataFile localDataFileCopy = new DataFile(df.getDataFilePath());

      // check if this key is present in the sparse index
      String locationPointerStr = index.get(key);
      if (locationPointerStr != null) {
        Long locationPointer = Long.parseLong(locationPointerStr);
        Data data = localDataFileCopy.read(locationPointer);
        return data.getValue();
      }

      Pair<Data, Data> predSuccPair = index.getInorderSuccessorPredecessor(key);
      Data inorderPredecessor = predSuccPair.getFirst();
      Data inorderSuccessor = predSuccPair.getSecond();

      Long floorPointer =
          inorderPredecessor == null ? 0 : Long.parseLong(inorderPredecessor.getValue());
      Long ceilingPointer =
          inorderSuccessor == null
              ? localDataFileCopy.getFileSize()
              : Long.parseLong(inorderSuccessor.getValue());
      DataFileIterator it = new DataFileIterator(localDataFileCopy, floorPointer);

      while (it.hasNext()) {
        if (it.getReadLocation() > ceilingPointer) break;

        Data d = it.next();
        if (key.equals(d.getKey())) {
          return d.getValue();
        }
      }

      return null;
    }
  }

  /**
   * Start serializing data in a file STEP 01 : for the first, every [KEY_COUNT_PER_SEGMENT] and
   * last element of the in memory index, insert the key in the index, along with the location
   * pointer in the file for the corresponding index.
   *
   * @throws IOException
   */
  public void persistOnDisk() throws IOException {
    long startMillis = System.currentTimeMillis();
    this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 100_000);

    RBTree oldIndex = index;
    RBTree newIndex = new RBTree();
    List<Data> dataList = oldIndex.getAllElements();

    for (int i = 0; i < dataList.size(); i++) {
      Data data = dataList.get(i);
      long writeLocation = df.writeInBlocks(data); // write to file
      // update sparse index if a new data block has started
      if(writeLocation != -1){
        newIndex.insert(data.getKey(), Long.toString(writeLocation));
      }
      bloomFilter.put(data.getKey()); // add key to bloomFilter
    }
    df.close();// This is important else the last block will not be written to disk.

    index = newIndex;
    indexType = INDEX_TYPE.SPARSE_INDEX;

    long endMillis = System.currentTimeMillis();
    System.out.println("Serialization time : " + Long.toString(endMillis - startMillis) + " ms");
    walFW.close();
    System.out.println("Deleting WAL : " + writeAheadLogFile.getPath());
    writeAheadLogFile.delete();

  }

  public int getSizeInBytes() {
    return index.getSizeInBytes();
  }
}
