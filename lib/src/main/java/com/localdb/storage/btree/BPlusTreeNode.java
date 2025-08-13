package com.localdb.storage.btree;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class BPlusTreeNode<K, V> {
  protected List<K> keys;
  protected boolean isLeaf;
  protected int maxKeys;
  protected SerializationStrategy<K> keySerializer;
  protected SerializationStrategy<V> valueSerializer;
  protected Comparator<K> comparator;

  public BPlusTreeNode(
      boolean isLeaf,
      int maxKeys,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator) {
    this.keys = new ArrayList<>();
    this.isLeaf = isLeaf;
    this.maxKeys = maxKeys;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.comparator = comparator;
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public List<K> getKeys() {
    return keys;
  }

  public boolean isFull() {
    return keys.size() >= maxKeys;
  }

  public boolean isUnderflow() {
    return keys.size() < maxKeys / 2;
  }

  protected int findKeyIndex(K key) {
    int left = 0;
    int right = keys.size() - 1;

    while (left <= right) {
      int mid = (left + right) / 2;
      int cmp = comparator.compare(key, keys.get(mid));

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

  public abstract SearchResult<V> search(K key);

  public abstract InsertResult<K, V> insert(K key, V value);

  public abstract DeleteResult<K, V> delete(K key);

  public abstract byte[] serialize() throws IOException;

  public abstract void deserialize(byte[] data) throws IOException;
}
