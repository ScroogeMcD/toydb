package com.example.toydb;

import com.example.toydb.common.ByteUtil;

public class Data {

    private String key; // key = Test012 =>
    private String value;
    private static final String HEX_KEY_VALUE_SEPARATOR = "00";

    public Data(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public Data(){
        key = "";
        value = "";
    }

    public Data(byte[] byteArr){
        deserialize(byteArr);
    }

    public byte[] serialize() {
        String keyHexString = stringToHex(key);
        String valueHexString = stringToHex(value);
        String keySeparatorValueHexString = keyHexString + HEX_KEY_VALUE_SEPARATOR + valueHexString;
        return ByteUtil.hexToBytes(keySeparatorValueHexString);
    }

    public void deserialize(byte[] byteArr) {
        String keySeparatorValueHexString = ByteUtil.bytesToHex(byteArr);


        // find the index of the separator in the hex string
        boolean prevZero = false;
        int separatorIndex = -1;
        for(int i=0; i< keySeparatorValueHexString.length(); i+=2){
            /*if(keySeparatorValueHexString.charAt(i) == '0'){
                if(prevZero){
                    separatorIndex = i-1;
                    break; //separator found
                }
                else
                    prevZero = true;
            }*/
            if(keySeparatorValueHexString.charAt(i) == '0' && keySeparatorValueHexString.charAt(i+1) == '0')
                separatorIndex = i;
        }

        if(separatorIndex == -1)
            throw new IllegalArgumentException("This byte array does not represent a valid key value pair. No separator found."
                    + keySeparatorValueHexString);

        String keyHexString = keySeparatorValueHexString.substring(0,separatorIndex);
        String valueHexString = keySeparatorValueHexString.substring(separatorIndex+2);

        key = hexToString(keyHexString);
        value = hexToString(valueHexString);
    }

    public String stringToHex(String str){
        StringBuilder sb = new StringBuilder();
        for(int i=0; i < str.length(); i++) {
            char c = str.charAt(i);
            int asciiValue = (int) c;
            sb.append(Integer.toHexString(asciiValue));
        }

        return sb.toString();
    }

    public String hexToString(String hexStr){
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<hexStr.length(); i += 2){
            String hexSubstring = hexStr.substring(i, i+2);
            int asciiValue = Integer.parseInt(hexSubstring, 16);
            char c = (char) asciiValue;
            sb.append(c);
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return "Data{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
