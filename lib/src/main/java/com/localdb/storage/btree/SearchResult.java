package com.localdb.storage.btree;

public class SearchResult<V> {
  private final boolean found;
  private final V value;

  private SearchResult(boolean found, V value) {
    this.found = found;
    this.value = value;
  }

  public static <V> SearchResult<V> found(V value) {
    return new SearchResult<>(true, value);
  }

  public static <V> SearchResult<V> notFound() {
    return new SearchResult<>(false, null);
  }

  public boolean isFound() {
    return found;
  }

  public V getValue() {
    return value;
  }
}
