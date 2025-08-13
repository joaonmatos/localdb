package com.localdb.storage.btree;

public record SearchResult<V>(boolean found, V value) {

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
