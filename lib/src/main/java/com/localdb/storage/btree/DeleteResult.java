package com.localdb.storage.btree;

public record DeleteResult<K, V>(boolean deleted, boolean underflow, K borrowedKey) {

  public static <K, V> DeleteResult<K, V> success() {
    return new DeleteResult<>(true, false, null);
  }

  public static <K, V> DeleteResult<K, V> notFound() {
    return new DeleteResult<>(false, false, null);
  }

  public static <K, V> DeleteResult<K, V> underflow(K borrowedKey) {
    return new DeleteResult<>(true, true, borrowedKey);
  }

  public boolean isDeleted() {
    return deleted;
  }

  public boolean isUnderflow() {
    return underflow;
  }

  public K getBorrowedKey() {
    return borrowedKey;
  }
}
