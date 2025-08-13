package com.localdb.storage.btree;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeInternalNode<K, V> extends BPlusTreeNode<K, V> {
  private List<BPlusTreeNode<K, V>> children;

  public BPlusTreeInternalNode(
      int maxKeys,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator) {
    super(false, maxKeys, keySerializer, valueSerializer, comparator);
    this.children = new ArrayList<>();
  }

  @Override
  public SearchResult<V> search(K key) {
    var index = 0;
    while (index < keys.size() && comparator.compare(key, keys.get(index)) >= 0) {
      index++;
    }
    return children.get(index).search(key);
  }

  @Override
  public InsertResult<K, V> insert(K key, V value) {
    var index = 0;
    while (index < keys.size() && comparator.compare(key, keys.get(index)) >= 0) {
      index++;
    }
    var result = children.get(index).insert(key, value);

    if (result.isSplitOccurred()) {
      keys.add(index, result.getPromotedKey());
      children.add(index + 1, result.getNewNode());

      if (keys.size() > maxKeys) {
        return split();
      }
    }

    return InsertResult.success();
  }

  private InsertResult<K, V> split() {
    var mid = keys.size() / 2;

    var newNode = new BPlusTreeInternalNode<>(maxKeys, keySerializer, valueSerializer, comparator);

    var promotedKey = keys.get(mid);

    newNode.keys.addAll(keys.subList(mid + 1, keys.size()));
    newNode.children.addAll(children.subList(mid + 1, children.size()));

    keys.subList(mid, keys.size()).clear();
    children.subList(mid + 1, children.size()).clear();

    return InsertResult.split(promotedKey, newNode);
  }

  @Override
  public DeleteResult<K, V> delete(K key) {
    var index = 0;
    while (index < keys.size() && comparator.compare(key, keys.get(index)) >= 0) {
      index++;
    }
    var result = children.get(index).delete(key);

    if (result.isDeleted() && result.isUnderflow()) {
      return handleChildUnderflow(index);
    }

    return result;
  }

  private DeleteResult<K, V> handleChildUnderflow(int childIndex) {
    var child = children.get(childIndex);

    if (childIndex > 0 && !children.get(childIndex - 1).isUnderflow()) {
      borrowFromLeftSibling(childIndex);
    } else if (childIndex < children.size() - 1 && !children.get(childIndex + 1).isUnderflow()) {
      borrowFromRightSibling(childIndex);
    } else {
      mergeWithSibling(childIndex);
    }

    if (isUnderflow()) {
      return DeleteResult.underflow(keys.isEmpty() ? null : keys.get(0));
    }

    return DeleteResult.success();
  }

  private void borrowFromLeftSibling(int childIndex) {}

  private void borrowFromRightSibling(int childIndex) {}

  private void mergeWithSibling(int childIndex) {}

  @Override
  public byte[] serialize() throws IOException {
    var baos = new ByteArrayOutputStream();

    baos.write(ByteBuffer.allocate(4).putInt(keys.size()).array());

    for (var key : keys) {
      var keyBytes = keySerializer.serialize(key);
      baos.write(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
      baos.write(keyBytes);
    }

    for (var child : children) {
      var childBytes = child.serialize();
      baos.write(ByteBuffer.allocate(4).putInt(childBytes.length).array());
      baos.write(childBytes);
    }

    return baos.toByteArray();
  }

  @Override
  public void deserialize(byte[] data) throws IOException {
    var buffer = ByteBuffer.wrap(data);

    var keyCount = buffer.getInt();
    keys.clear();

    for (int i = 0; i < keyCount; i++) {
      var keyLength = buffer.getInt();
      var keyBytes = new byte[keyLength];
      buffer.get(keyBytes);
      keys.add(keySerializer.deserialize(keyBytes));
    }

    children.clear();
    for (int i = 0; i <= keyCount; i++) {
      var childLength = buffer.getInt();
      var childBytes = new byte[childLength];
      buffer.get(childBytes);
    }
  }

  public List<BPlusTreeNode<K, V>> getChildren() {
    return children;
  }

  public void addChild(BPlusTreeNode<K, V> child) {
    children.add(child);
  }
}
