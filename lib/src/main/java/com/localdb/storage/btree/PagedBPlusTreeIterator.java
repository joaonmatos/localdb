package com.localdb.storage.btree;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator for range queries in a paged B+Tree that lazily loads pages from disk. This allows
 * efficient iteration over large result sets without loading all data into memory.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public final class PagedBPlusTreeIterator<K, V> implements Iterator<V> {

  private final com.localdb.storage.Comparator<K> comparator;
  private final K endKey;
  private PagedBPlusTreeLeafNode<K, V> currentLeaf;
  private int currentIndex;
  private boolean hasNextCached;
  private boolean hasNextValue;

  /**
   * Creates a new iterator starting from the specified leaf node.
   *
   * @param startLeaf the leaf node to start iteration from
   * @param startIndex the index within the start leaf to begin at
   * @param endKey the end key of the range (inclusive)
   * @param comparator the key comparator
   */
  public PagedBPlusTreeIterator(
      PagedBPlusTreeLeafNode<K, V> startLeaf,
      int startIndex,
      K endKey,
      com.localdb.storage.Comparator<K> comparator) {
    this.currentLeaf = startLeaf;
    this.currentIndex = startIndex;
    this.endKey = endKey;
    this.comparator = comparator;
    this.hasNextCached = false;
  }

  @Override
  public boolean hasNext() {
    if (!hasNextCached) {
      hasNextValue = computeHasNext();
      hasNextCached = true;
    }
    return hasNextValue;
  }

  @Override
  public V next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    var value = currentLeaf.getValues().get(currentIndex);

    // Advance to next position
    currentIndex++;
    hasNextCached = false; // Invalidate cache

    return value;
  }

  /**
   * Computes whether there are more elements to iterate over. This method handles lazy loading of
   * leaf nodes from disk.
   *
   * @return true if there are more elements
   */
  private boolean computeHasNext() {
    try {
      // Check if we have more elements in the current leaf
      while (currentLeaf != null) {
        var keys = currentLeaf.getKeys();

        if (currentIndex < keys.size()) {
          var key = keys.get(currentIndex);

          // Check if key is within range
          if (comparator.compare(key, endKey) <= 0) {
            return true;
          } else {
            // Key exceeds end range
            return false;
          }
        }

        // Move to next leaf node (lazy loading from disk)
        currentLeaf = currentLeaf.getNextLeaf();
        currentIndex = 0;
      }

      return false;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load next leaf node from disk", e);
    }
  }
}
