package toydb.index;

import java.util.HashMap;
import java.util.Map;

public class HashBasedIndex implements DBIndex {

  private Map<String, Long> keyToLocationMap;

  public HashBasedIndex() {
    keyToLocationMap = new HashMap<>();
  }

  @Override
  public Long getLocationFromIndex(String key) {
    return keyToLocationMap.get(key);
  }

  @Override
  public void updateIndex(String key, Long value) {
    keyToLocationMap.put(key, value);
  }
}
