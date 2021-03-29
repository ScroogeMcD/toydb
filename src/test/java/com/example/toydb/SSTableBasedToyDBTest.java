package com.example.toydb;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SSTableBasedToyDBTest {



    @Test
    public void simpleTest() throws FileNotFoundException, InterruptedException {



        ExecutorService executorService = Executors.newFixedThreadPool(20);
        File dir = new File("/Users/priyanka/MyTestDB");
        for(File f: dir.listFiles())
            f.delete();
        ToyDB db = new SSTableBasedToyDB("/Users/priyanka/MyTestDB");

        long writeStartMillis = System.currentTimeMillis();
        for(int i=1; i<= 100_000; i++){
            db.put("Key"+i,"Value"+i);
        }
        long writeEndMillis = System.currentTimeMillis();

        //System.out.println("Mismatch | Key:key5000"+ "\tExpectedValue:null"+ "\tObtainedValue:"+db.get("5000"));

        AtomicInteger mismatches = new AtomicInteger(0);
        AtomicInteger matches = new AtomicInteger(0);

        for(int i=1; i<= 100_000; i++){

            executorService.submit(new GetValueTask(i,db, matches, mismatches));
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
        System.out.println("Matches : "+matches + "\t Mismatches:"+mismatches);
        long readEndMillis = System.currentTimeMillis();

        System.out.println("Write time : " + Long.toString(writeEndMillis-writeStartMillis) + ", Read Time : " + Long.toString(readEndMillis-writeEndMillis));

    }
}

class GetValueTask implements Callable<String> {

    private int i;
    private ToyDB toyDB;
    AtomicInteger matches;
    AtomicInteger mismatches;

    public GetValueTask(int i, ToyDB toyDB, AtomicInteger matches, AtomicInteger mismatches){
        this.i = i;
        this.toyDB = toyDB;
        this.matches = matches;
        this.mismatches = mismatches;
    }

    @Override
    public String call() throws Exception {
        String key = "Key"+i;
        String expectedValue = "Value"+i;
        String obtainedValue = toyDB.get(key);
        if(obtainedValue == null || expectedValue.compareTo(obtainedValue) != 0){
            System.out.println("Mismatch | Key:"+key + "\tExpectedValue:"+expectedValue + "\tObtainedValue:"+obtainedValue);
            mismatches.incrementAndGet();
        }else
            matches.incrementAndGet();

        return obtainedValue;
    }
}
