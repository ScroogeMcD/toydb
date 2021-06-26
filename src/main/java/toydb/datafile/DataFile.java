package toydb.datafile;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;
import toydb.toydb.Data;

/**
 * Data or payload to the file will be written in the following format :
 * size_in_bytes_of_the_payload followed by the payload
 *
 * <p>The first two bytes would represent the size in bytes [say N] of the payload, and the next N
 * bytes would represent the actual payload.
 *
 * <p>In two bytes, we can represent a length of upto 2^16-1, which is 65,535. [Determine what is
 * the max total size of key+val that we can support].
 */
public class DataFile implements Iterable<Data>, Closeable {

  private static final int BLOCK_SIZE_IN_BYTES = 4 * 1024; // 4KB
  private int currBlockStartPointerInFile = 0;
  private ByteBuffer bb= ByteBuffer.allocate(BLOCK_SIZE_IN_BYTES);
  private String dataFilePath;

  private RandomAccessFile file;

  public DataFile(String filePath) throws FileNotFoundException {
    this.dataFilePath = filePath;
    this.file = new RandomAccessFile(dataFilePath, "rw");
  }

  /**
   * Writes data to the underlying file. This method buffers the writes in memory, and actually flushes them on the file
   * only when the size of the block reaches BLOCK_SIZE.
   *
   * @param d
   * @return The pointer starting which the data was written. This should later be used for querying
   *     the data.
   * @throws IOException
   */
  public long writeInBlocks(Data d) throws IOException {
    byte[] payload = d.serialize();

    // If addition of this key can cause the block to overflow, flush this block to disk, and create a new block, to
    // which this new data will be written.
    int addressOfCurrBlock = -1;
    if(causesBlockOverflow(payload.length)){
      file.write(bb.array());
      bb = ByteBuffer.allocate(BLOCK_SIZE_IN_BYTES);
      currBlockStartPointerInFile += BLOCK_SIZE_IN_BYTES;
    }

    // If a new ByteBuffer has been created, the new block's start address needs to be returned
    if(bb.position() == 0){
      addressOfCurrBlock = currBlockStartPointerInFile;
    }
    writeToByteBuffer(payload);
    return addressOfCurrBlock;
  }

  private void writeToByteBuffer(byte[] payload){
    short payloadSize = (short) payload.length;
    byte payloadSizeByteFirst = (byte) (payloadSize & 0xFF00);
    byte payloadSizeByteSecond = (byte) (payloadSize & 0x00FF);
    bb.put(payloadSizeByteFirst).put(payloadSizeByteSecond).put(payload);
  }

  public long write(Data d) throws IOException {
    byte[] payload = d.serialize();
    long appendLocation = file.length();
    file.seek(appendLocation);
    ByteBuffer byteBuffer = ByteBuffer.allocate(2 + payload.length);
    short payloadSize = (short) payload.length;

    byte payloadSizeByteFirst = (byte) (payloadSize & 0xFF00);
    byte payloadSizeByteSecond = (byte) (payloadSize & 0x00FF);

    byteBuffer.put(0, payloadSizeByteFirst);
    byteBuffer.put(1, payloadSizeByteSecond);
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

  private boolean causesBlockOverflow(int payloadLength){
    return (bb.position() + (2 + payloadLength)) > bb.capacity();
  }

  public byte[] readBytes(long location) throws IOException {
    file.seek(location);
    byte[] payloadSizeArr = new byte[2];
    file.read(payloadSizeArr);

    short payloadSize = (short) (payloadSizeArr[0] | payloadSizeArr[1]);
    byte[] payload = new byte[payloadSize];
    file.seek(location + 2);
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

  @Override
  public void close() throws IOException {
    file.write(bb.array());
  }

  public String getDataFilePath() {
    return dataFilePath;
  }
}
