package com.localdb.storage.btree;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A B+ tree implementation that provides efficient ordered storage and retrieval of key-value
 * pairs. B+ trees are balanced trees where all data is stored in leaf nodes, and internal nodes
 * contain only keys for navigation.
 *
 * <p>This implementation features:
 *
 * <ul>
 *   <li>Balanced tree structure ensuring O(log n) operations
 *   <li>Range query support through linked leaf nodes
 *   <li>Configurable tree order for performance tuning
 *   <li>Custom key comparators for flexible ordering
 *   <li>Serialization support for persistence
 * </ul>
 *
 * <p>The tree automatically maintains balance through node splitting and merging operations during
 * insertions and deletions.
 *
 * @param <K> the type of keys stored in the tree
 * @param <V> the type of values stored in the tree
 */
public class BPlusTree<K, V> {
  private BPlusTreeNode<K, V> root;
  private final int order;
  private final SerializationStrategy<K> keySerializer;
  private final SerializationStrategy<V> valueSerializer;
  private final Comparator<K> comparator;

  public BPlusTree(
      int order,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator) {
    this.order = order;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.comparator = comparator;
    this.root = new BPlusTreeLeafNode<>(order, keySerializer, valueSerializer, comparator);
  }

  /**
   * Searches for a value associated with the specified key.
   *
   * @param key the key to search for
   * @return an Optional containing the value if found, empty otherwise
   */
  public Optional<V> search(K key) {
    var result = root.search(key);
    return result.isFound() ? Optional.of(result.getValue()) : Optional.empty();
  }

  /**
   * Inserts or updates a key-value pair in the tree. If the key already exists, its value is
   * updated.
   *
   * @param key the key to insert
   * @param value the value to associate with the key
   */
  public void insert(K key, V value) {
    var result = root.insert(key, value);

    if (result.isSplitOccurred()) {
      var newRoot = new BPlusTreeInternalNode<>(order, keySerializer, valueSerializer, comparator);
      newRoot.getKeys().add(result.getPromotedKey());
      newRoot.addChild(root);
      newRoot.addChild(result.getNewNode());
      root = newRoot;
    }
  }

  /**
   * Deletes the key-value pair associated with the specified key.
   *
   * @param key the key to delete
   * @return true if the key was found and deleted, false otherwise
   */
  public boolean delete(K key) {
    var result = root.delete(key);

    if (!root.isLeaf() && root.getKeys().isEmpty()) {
      var internalRoot = (BPlusTreeInternalNode<K, V>) root;
      if (!internalRoot.getChildren().isEmpty()) {
        root = internalRoot.getChildren().get(0);
      }
    }

    return result.isDeleted();
  }

  /**
   * Performs a range query, returning all values for keys in the specified range.
   *
   * @param startKey the start of the range (inclusive)
   * @param endKey the end of the range (inclusive)
   * @return a list of values in the specified range, in key order
   */
  public List<V> rangeQuery(K startKey, K endKey) {
    var result = new ArrayList<V>();
    var current = findFirstLeaf(startKey);

    while (current != null) {
      for (int i = 0; i < current.getKeys().size(); i++) {
        var key = current.getKeys().get(i);

        if (comparator.compare(key, startKey) >= 0 && comparator.compare(key, endKey) <= 0) {
          result.add(current.getValues().get(i));
        } else if (comparator.compare(key, endKey) > 0) {
          return result;
        }
      }
      current = current.getNext();
    }

    return result;
  }

  private BPlusTreeLeafNode<K, V> findFirstLeaf(K key) {
    var current = root;

    while (!current.isLeaf()) {
      var internalNode = (BPlusTreeInternalNode<K, V>) current;
      var index = findKeyIndex(current, key);
      current = internalNode.getChildren().get(index);
    }

    return (BPlusTreeLeafNode<K, V>) current;
  }

  private int findKeyIndex(BPlusTreeNode<K, V> node, K key) {
    var left = 0;
    var right = node.getKeys().size() - 1;

    while (left <= right) {
      var mid = (left + right) / 2;
      var cmp = comparator.compare(key, node.getKeys().get(mid));

      if (cmp == 0) {
        return mid;
      } else if (cmp < 0) {
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }

    return left;
  }

  public byte[] serialize() throws IOException {
    return root.serialize();
  }

  public void deserialize(byte[] data) throws IOException {
    root = new BPlusTreeLeafNode<>(order, keySerializer, valueSerializer, comparator);
    root.deserialize(data);
  }

  /**
   * Returns true if this tree contains no key-value pairs.
   *
   * @return true if the tree is empty
   */
  public boolean isEmpty() {
    return root.getKeys().isEmpty();
  }

  /**
   * Returns the number of key-value pairs in this tree.
   *
   * @return the number of key-value pairs
   */
  public int size() {
    return countKeys(root);
  }

  private int countKeys(BPlusTreeNode<K, V> node) {
    if (node.isLeaf()) {
      return node.getKeys().size();
    } else {
      var count = 0;
      var internalNode = (BPlusTreeInternalNode<K, V>) node;
      for (var child : internalNode.getChildren()) {
        count += countKeys(child);
      }
      return count;
    }
  }
}
