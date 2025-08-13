package com.localdb.storage.btree;

public record InsertResult<K, V>(
    boolean splitOccurred, K promotedKey, BPlusTreeNode<K, V> newNode) {

  public static <K, V> InsertResult<K, V> success() {
    return new InsertResult<>(false, null, null);
  }

  public static <K, V> InsertResult<K, V> split(K promotedKey, BPlusTreeNode<K, V> newNode) {
    return new InsertResult<>(true, promotedKey, newNode);
  }

  public boolean isSplitOccurred() {
    return splitOccurred;
  }

  public K getPromotedKey() {
    return promotedKey;
  }

  public BPlusTreeNode<K, V> getNewNode() {
    return newNode;
  }
}
