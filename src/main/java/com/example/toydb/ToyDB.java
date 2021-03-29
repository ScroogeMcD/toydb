package com.example.toydb;

public interface ToyDB {

    public void put(String key, String value);

    public String get(String key);

    public void delete(String key);
}
