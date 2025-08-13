package com.localdb.storage.btree;

/**
 * Result of a delete operation in a paged B+Tree node.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @param deleted whether the key was found and deleted
 * @param underflow whether the node underflowed after deletion
 * @param firstKey the first key in the node after deletion (for parent updates)
 */
public record PagedDeleteResult<K, V>(boolean deleted, boolean underflow, K firstKey) {

  /**
   * Creates a successful delete result with no underflow.
   *
   * @param <K> the type of keys
   * @param <V> the type of values
   * @return a successful delete result
   */
  public static <K, V> PagedDeleteResult<K, V> success() {
    return new PagedDeleteResult<>(true, false, null);
  }

  /**
   * Creates a delete result indicating the key was not found.
   *
   * @param <K> the type of keys
   * @param <V> the type of values
   * @return a not found delete result
   */
  public static <K, V> PagedDeleteResult<K, V> notFound() {
    return new PagedDeleteResult<>(false, false, null);
  }

  /**
   * Creates a delete result indicating underflow occurred.
   *
   * @param <K> the type of keys
   * @param <V> the type of values
   * @param firstKey the first key in the node after deletion
   * @return an underflow delete result
   */
  public static <K, V> PagedDeleteResult<K, V> underflow(K firstKey) {
    return new PagedDeleteResult<>(true, true, firstKey);
  }

  /**
   * Checks if the key was successfully deleted.
   *
   * @return true if the key was deleted
   */
  public boolean isDeleted() {
    return deleted;
  }

  /**
   * Checks if the node underflowed after deletion.
   *
   * @return true if underflow occurred
   */
  public boolean isUnderflow() {
    return underflow;
  }

  /**
   * Gets the first key in the node after deletion.
   *
   * @return the first key, or null if the node is empty
   */
  public K getFirstKey() {
    return firstKey;
  }
}
