package com.example.toydb;

import com.example.toydb.com.example.toydb.index.Memtable;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class SSTableBasedToyDB implements ToyDB {

    List<Memtable> memTables;

    String dbDirectoryPath;

    private static final Integer SSTABLE_MAX_SIZE_IN_BYTES = 10 * 10_000;// Assuming size of one KV as 10 bytes, so 10000 KV stored here

    public SSTableBasedToyDB(String dbDirectoryPath) throws FileNotFoundException {
        this.dbDirectoryPath = dbDirectoryPath;
        initDB();
    }

    private void initDB() throws FileNotFoundException {
        // STEP 01 : read all the serialized files from disk and create the SPARSE memTables list
        memTables = new ArrayList<>(); // read from disk

        // STEP 02 : read from the writeAheadLog and create the current DENSE memtable
        memTables.add(0, new Memtable(dbDirectoryPath,"sortedStringFile_"+ memTables.size()));
    }

    @Override
    public void put(String key, String value) {
        Memtable currMemTable = memTables.get(0);

        if(currMemTable.getSizeInBytes() + key.length() + value.length() > SSTABLE_MAX_SIZE_IN_BYTES){
            try {
                //System.out.println("Serializing current memtable to file on disk."); // this can be done in the background
                currMemTable.serialize();
                //System.out.println("Creating a new memtable");
                currMemTable = new Memtable(dbDirectoryPath,"sortedStringFile_"+ memTables.size());
                memTables.add(0,currMemTable);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try{
            currMemTable.put(key, value);
        }catch(Exception e){
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
        }catch(Exception e){
            System.out.print("Exception encountered while searching for key : " + key + " | " + e);
        }

        return value;
    }

    @Override
    public void delete(String key) {

    }
}
