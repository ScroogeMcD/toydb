package com.example.toydb.common;

public class ByteUtil {

    public static final String HEX_CHARS = "0123456789ABCDEF";


    /**
     * How does one convert a character to hex, can be different based on encodings,
     * but conversion from hex to byte, and byte to hex, will be the same across languages.
     */

    /** converts a string of hexadecimal to bytes : one hex character requires 4 bits,
     * so two hex-characters is one byte
     * example : Test => 0x 54 65 73 74
     */
    public static byte[] hexToBytes(String hexStr) {
        // TODO : check that the hexStr is of even length
        byte[] bytes = new byte[hexStr.length()/2];

        for(int i=0; i< hexStr.length(); i += 2){
            byte leftNibble = (byte) ( (0x0F & Character.digit(hexStr.charAt(i),16)) <<4);
            byte rightNibble = (byte) (0x0F & Character.digit(hexStr.charAt(i+1),16));
            byte b = (byte) ( leftNibble | rightNibble );
            bytes[i/2]=b;
        }

        return bytes;
    }

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length*2];

        for(int i=0; i<bytes.length; i++){
            int leftNibble = (bytes[i] & 0xF0) >> 4;  // 0xF0 = 1111 0000
            int rightNibble = bytes[i] & 0x0F;  // 0x0F = 0000 1111

            hexChars[2*i] = HEX_CHARS.charAt(leftNibble);
            hexChars[2*i+1] = HEX_CHARS.charAt(rightNibble);
        }

        return new String(hexChars);
    }
}
