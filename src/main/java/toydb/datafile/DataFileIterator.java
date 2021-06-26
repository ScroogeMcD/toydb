package toydb.datafile;

import java.io.IOException;
import java.util.Iterator;
import toydb.toydb.Data;

public class DataFileIterator implements Iterator<Data> {

  private DataFile f;
  private long readLocation = 0;

  public DataFileIterator(DataFile f) {
    this.f = f;
  }

  public DataFileIterator(DataFile f, long readLocation) {
    this.f = f;
    this.readLocation = readLocation;
  }

  @Override
  public boolean hasNext() {
    try {
      return readLocation < f.getFileSize();
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public Data next() {
    Data d = new Data();
    try {
      byte[] payload = f.readBytes(readLocation);
      readLocation += payload.length + 2;
      d.deserialize(payload);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return d;
  }

  public long getReadLocation() {
    return readLocation;
  }
}
