package com.localdb.storage.btree;

public class DeleteResult<K, V> {
  private final boolean deleted;
  private final boolean underflow;
  private final K borrowedKey;

  private DeleteResult(boolean deleted, boolean underflow, K borrowedKey) {
    this.deleted = deleted;
    this.underflow = underflow;
    this.borrowedKey = borrowedKey;
  }

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
