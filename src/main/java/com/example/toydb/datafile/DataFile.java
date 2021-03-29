package com.example.toydb.datafile;

import com.example.toydb.Data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;


/**
 * Data or payload to the file will be written in the following format :
 * size_in_bytes_of_the_payload followed by the payload
 *
 * The first two bytes would represent the size in bytes [say N] of the payload, and the next N bytes would
 * represent the actual payload.
 *
 * In two bytes, we can represent a length of upto 2^16-1, which is 65,535.
 * [Determine what is the max total size of key+val that we can support].
 */
public class DataFile implements Iterable<Data> {

    private RandomAccessFile file;

    public DataFile(String filePath) throws FileNotFoundException {
        this.file = new RandomAccessFile(filePath,"rw");
    }

    /**
     * Writes data to the underlying file.
     *
     * @param d
     * @return The pointer starting which the data was written. This should later be used for querying the data.
     * @throws IOException
     */
    public long write(Data d) throws IOException {
        byte[] payload = d.serialize();
        long appendLocation = file.length();
        file.seek(appendLocation);
        ByteBuffer byteBuffer = ByteBuffer.allocate(2 + payload.length);
        short payloadSize = (short) payload.length;

        byte payloadSizeByteFirst = (byte) (payloadSize & 0xFF00);
        byte payloadSizeByteSecond = (byte) (payloadSize & 0x00FF);

        byteBuffer.put(0,payloadSizeByteFirst);
        byteBuffer.put(1,payloadSizeByteSecond);
        byteBuffer.put(2, payload);
        file.write(byteBuffer.array());
        return appendLocation;
    }

    public Data read(long location) throws IOException {
        byte[] payload = readBytes(location);

        Data d = new Data();
        d.deserialize(payload);
        return d;
    }

    public byte[] readBytes(long location) throws IOException {
        file.seek(location);
        byte[] payloadSizeArr = new byte[2];
        file.read(payloadSizeArr);

        short payloadSize = (short) (payloadSizeArr[0] | payloadSizeArr[1]);
        byte[] payload = new byte[payloadSize];
        file.seek(location+2);
        file.read(payload);

        return payload;
    }

    public Iterator<Data> iterator() {
        return new DataFileIterator(this);
    }

    public Iterator<Data> iterator(long readStartLocation) {
        return new DataFileIterator(this, readStartLocation);
    }

    public long getFileSize() throws IOException {
        return file.length();
    }
}
