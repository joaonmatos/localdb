package com.localdb.storage.btree;

public class InsertResult<K, V> {
  private final boolean splitOccurred;
  private final K promotedKey;
  private final BPlusTreeNode<K, V> newNode;

  private InsertResult(boolean splitOccurred, K promotedKey, BPlusTreeNode<K, V> newNode) {
    this.splitOccurred = splitOccurred;
    this.promotedKey = promotedKey;
    this.newNode = newNode;
  }

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
