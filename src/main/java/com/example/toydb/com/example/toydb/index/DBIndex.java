package com.example.toydb.com.example.toydb.index;

public interface DBIndex {

    public Long getLocationFromIndex(String key);

    /* stores a mapping of key to its corresponding starting location in db file */
    public void updateIndex(String key, Long value);
}
