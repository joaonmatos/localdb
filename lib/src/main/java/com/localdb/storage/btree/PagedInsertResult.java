package com.localdb.storage.btree;

/**
 * Result of an insert operation in a paged B+Tree node.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @param splitOccurred whether a split occurred during the insert
 * @param promotedKey the key promoted to the parent during a split
 * @param newNode the new node created during a split
 */
public record PagedInsertResult<K, V>(
    boolean splitOccurred, K promotedKey, PagedBPlusTreeNode<K, V> newNode) {

  /**
   * Creates a successful insert result with no split.
   *
   * @param <K> the type of keys
   * @param <V> the type of values
   * @return a successful insert result
   */
  public static <K, V> PagedInsertResult<K, V> success() {
    return new PagedInsertResult<>(false, null, null);
  }

  /**
   * Creates an insert result indicating a split occurred.
   *
   * @param <K> the type of keys
   * @param <V> the type of values
   * @param promotedKey the key promoted to the parent
   * @param newNode the new node created during the split
   * @return a split insert result
   */
  public static <K, V> PagedInsertResult<K, V> split(
      K promotedKey, PagedBPlusTreeNode<K, V> newNode) {
    return new PagedInsertResult<>(true, promotedKey, newNode);
  }

  /**
   * Checks if a split occurred during the insert.
   *
   * @return true if a split occurred
   */
  public boolean isSplitOccurred() {
    return splitOccurred;
  }

  /**
   * Gets the key promoted to the parent during a split.
   *
   * @return the promoted key, or null if no split occurred
   */
  public K getPromotedKey() {
    return promotedKey;
  }

  /**
   * Gets the new node created during a split.
   *
   * @return the new node, or null if no split occurred
   */
  public PagedBPlusTreeNode<K, V> getNewNode() {
    return newNode;
  }
}
