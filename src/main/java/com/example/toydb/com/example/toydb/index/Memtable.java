package com.example.toydb.com.example.toydb.index;

import com.example.toydb.Data;
import com.example.toydb.common.Pair;
import com.example.toydb.common.RBTree;
import com.example.toydb.datafile.DataFile;
import com.example.toydb.datafile.DataFileIterator;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import kotlin.text.Charsets;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * This class represents an in memory table, and its associated on-disk file.
 *
 * The memtable contains an in memory table of key_value pairs, and when the table gets filled up,
 * it is serialized and written on the disk.
 */
public class Memtable {

    private DataFile df;
    private String datafilePath;
    private RBTree index;
    private Path writeAheadLogFilePath;
    private INDEX_TYPE indexType;
    private BloomFilter<String> bloomFilter;

    enum INDEX_TYPE {DENSE_INDEX, SPARSE_INDEX}

    private static final int KEY_COUNT_PER_SEGMENT = 25;

    public Memtable(String dataFileBasePath, String dataFileName) throws FileNotFoundException {
        datafilePath = dataFileBasePath + "/" + dataFileName;
        writeAheadLogFilePath = Paths.get(dataFileBasePath + "/write_ahead_log_" + dataFileName + ".txt");
        this.index = new RBTree();
        indexType = INDEX_TYPE.DENSE_INDEX;
    }

    public void put(String key, String value) throws IOException {
        Files.write(writeAheadLogFilePath, new Data(key,value).serialize(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        index.insert(key, value);
    }

    public String get(String key) throws IOException {
        // STEP 01 : If it is DENSE_INDEX, then check for key,value in memory
        if(indexType == INDEX_TYPE.DENSE_INDEX)
            return index.get(key);
        /*
            STEP 02 : It is a SPARSE INDEX
            - First check if this key is present in the sparse index itself. If yes, then get the location pointer from memory,
              and read the value at this location.
            - If key not present in sparse index, get the predecessor and successor location pointers for this key, from sparse index.
            - Iterate between this predecessor and successor on the desk, and see if this key is found. If found, return value.
            - If not, return null.
         */
        else{
            // If not present in BloomFilter return null
            if(!bloomFilter.mightContain(key)) return null;

            DataFile localDataFileCopy = new DataFile(datafilePath);

            //check if this key is present in the sparse index
            String locationPointerStr = index.get(key);
            if(locationPointerStr != null){
                Long locationPointer = Long.parseLong(locationPointerStr);
                Data data  = localDataFileCopy.read(locationPointer);
                return data.getValue();
            }


            Pair<Data, Data> predSuccPair = index.getInorderSuccessorPredecessor(key);
            Data inorderPredecessor = predSuccPair.getFirst();
            Data inorderSuccessor = predSuccPair.getSecond();

            Long floorPointer = inorderPredecessor == null ? 0 : Long.parseLong(inorderPredecessor.getValue());
            Long ceilingPointer = inorderSuccessor == null ? localDataFileCopy.getFileSize() : Long.parseLong(inorderSuccessor.getValue());
            DataFileIterator it = new DataFileIterator(localDataFileCopy,floorPointer);

            while(it.hasNext()){
                if(it.getReadLocation() > ceilingPointer ) break;

                Data d = it.next();
                if(key.equals(d.getKey())){
                     return d.getValue();
                 }
            }

            return null;
        }

    }

    /**
     *  Start serializing data in a file
     *  STEP 01 : for the first, every [KEY_COUNT_PER_SEGMENT] and last element of the in memory index, insert the key in the index,
     *            along with the location pointer in the file for the corresponding index.
     * @throws IOException
     */
    public void serialize() throws IOException {
        long startMillis = System.currentTimeMillis();
        this.df = new DataFile(datafilePath);
        this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8),100_000);

        RBTree oldIndex = index;
        RBTree newIndex = new RBTree();
        List<Data> dataList = oldIndex.getAllElements();

        for(int i=0; i< dataList.size(); i++){
            Data data = dataList.get(i);
            long writeLocation = df.write(data); // write to file
            bloomFilter.put(data.getKey()); // add key to bloomFilter

            //System.out.println("Writing to file: " + datafilePath + "\tData : " + dataList.get(i));
            if(i==0 || i%KEY_COUNT_PER_SEGMENT == 0 || i==dataList.size()){
                newIndex.insert(data.getKey(),Long.toString(writeLocation));
            }
        }

        index = newIndex;
        indexType = INDEX_TYPE.SPARSE_INDEX;

        long endMillis = System.currentTimeMillis();
        System.out.println("Serialization time : " + Long.toString(endMillis-startMillis) + " ms");
    }

    public int getSizeInBytes() {
        return index.getSizeInBytes();
    }
}
