package com.localdb.storage.btree;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeLeafNode<K, V> extends BPlusTreeNode<K, V> {
  private List<V> values;
  private BPlusTreeLeafNode<K, V> next;

  public BPlusTreeLeafNode(
      int maxKeys,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator) {
    super(true, maxKeys, keySerializer, valueSerializer, comparator);
    this.values = new ArrayList<>();
  }

  @Override
  public SearchResult<V> search(K key) {
    var index = findKeyIndex(key);
    if (index < keys.size() && comparator.compare(keys.get(index), key) == 0) {
      return SearchResult.found(values.get(index));
    }
    return SearchResult.notFound();
  }

  @Override
  public InsertResult<K, V> insert(K key, V value) {
    var index = findKeyIndex(key);

    if (index < keys.size() && comparator.compare(keys.get(index), key) == 0) {
      values.set(index, value);
      return InsertResult.success();
    }

    keys.add(index, key);
    values.add(index, value);

    if (keys.size() > maxKeys) {
      return split();
    }

    return InsertResult.success();
  }

  private InsertResult<K, V> split() {
    var mid = keys.size() / 2;

    var newNode = new BPlusTreeLeafNode<>(maxKeys, keySerializer, valueSerializer, comparator);

    newNode.keys.addAll(keys.subList(mid, keys.size()));
    newNode.values.addAll(values.subList(mid, values.size()));

    keys.subList(mid, keys.size()).clear();
    values.subList(mid, values.size()).clear();

    newNode.next = this.next;
    this.next = newNode;

    return InsertResult.split(newNode.keys.get(0), newNode);
  }

  @Override
  public DeleteResult<K, V> delete(K key) {
    var index = findKeyIndex(key);

    if (index >= keys.size() || comparator.compare(keys.get(index), key) != 0) {
      return DeleteResult.notFound();
    }

    keys.remove(index);
    values.remove(index);

    if (isUnderflow()) {
      return DeleteResult.underflow(keys.isEmpty() ? null : keys.get(0));
    }

    return DeleteResult.success();
  }

  @Override
  public byte[] serialize() throws IOException {
    var baos = new ByteArrayOutputStream();

    baos.write(ByteBuffer.allocate(4).putInt(keys.size()).array());

    for (int i = 0; i < keys.size(); i++) {
      var keyBytes = keySerializer.serialize(keys.get(i));
      var valueBytes = valueSerializer.serialize(values.get(i));

      baos.write(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
      baos.write(keyBytes);
      baos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
      baos.write(valueBytes);
    }

    return baos.toByteArray();
  }

  @Override
  public void deserialize(byte[] data) throws IOException {
    var buffer = ByteBuffer.wrap(data);

    var keyCount = buffer.getInt();
    keys.clear();
    values.clear();

    for (int i = 0; i < keyCount; i++) {
      var keyLength = buffer.getInt();
      var keyBytes = new byte[keyLength];
      buffer.get(keyBytes);

      var valueLength = buffer.getInt();
      var valueBytes = new byte[valueLength];
      buffer.get(valueBytes);

      keys.add(keySerializer.deserialize(keyBytes));
      values.add(valueSerializer.deserialize(valueBytes));
    }
  }

  public List<V> getValues() {
    return values;
  }

  public BPlusTreeLeafNode<K, V> getNext() {
    return next;
  }

  public void setNext(BPlusTreeLeafNode<K, V> next) {
    this.next = next;
  }
}
