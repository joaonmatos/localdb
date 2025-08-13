package com.localdb.storage.btree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BPlusTreeTest {

  private BPlusTree<Integer, String> btree;

  @BeforeEach
  void setUp() {
    btree =
        new BPlusTree<>(
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER);
  }

  @Test
  void testEmptyTree() {
    assertTrue(btree.isEmpty());
    assertEquals(0, btree.size());
    assertEquals(Optional.empty(), btree.search(1));
  }

  @Test
  void testSingleInsertAndSearch() {
    btree.insert(1, "one");

    assertFalse(btree.isEmpty());
    assertEquals(1, btree.size());
    assertEquals(Optional.of("one"), btree.search(1));
    assertEquals(Optional.empty(), btree.search(2));
  }

  @Test
  void testMultipleInsertsAndSearches() {
    btree.insert(3, "three");
    btree.insert(1, "one");
    btree.insert(4, "four");
    btree.insert(2, "two");

    assertEquals(4, btree.size());
    assertEquals(Optional.of("one"), btree.search(1));
    assertEquals(Optional.of("two"), btree.search(2));
    assertEquals(Optional.of("three"), btree.search(3));
    assertEquals(Optional.of("four"), btree.search(4));
    assertEquals(Optional.empty(), btree.search(5));
  }

  @Test
  void testUpdateValue() {
    btree.insert(1, "one");
    btree.insert(1, "ONE");

    assertEquals(1, btree.size());
    assertEquals(Optional.of("ONE"), btree.search(1));
  }

  @Test
  void testDelete() {
    btree.insert(1, "one");
    btree.insert(2, "two");
    btree.insert(3, "three");

    assertTrue(btree.delete(2));
    assertEquals(2, btree.size());
    assertEquals(Optional.empty(), btree.search(2));
    assertEquals(Optional.of("one"), btree.search(1));
    assertEquals(Optional.of("three"), btree.search(3));

    assertFalse(btree.delete(2));
    assertFalse(btree.delete(5));
  }

  @Test
  void testRangeQuery() {
    for (int i = 1; i <= 10; i++) {
      btree.insert(i, "value" + i);
    }

    List<String> result = btree.rangeQuery(3, 7);
    assertEquals(5, result.size());

    List<String> allResults = btree.rangeQuery(1, 10);
    assertEquals(10, allResults.size());

    List<String> emptyResult = btree.rangeQuery(15, 20);
    assertTrue(emptyResult.isEmpty());
  }

  @Test
  void testLargeDataset() {
    int count = 1000;
    for (int i = 0; i < count; i++) {
      btree.insert(i, "value" + i);
    }

    assertEquals(count, btree.size());

    for (int i = 0; i < count; i++) {
      assertEquals(Optional.of("value" + i), btree.search(i));
    }

    for (int i = 0; i < count; i += 2) {
      assertTrue(btree.delete(i));
    }

    assertEquals(count / 2, btree.size());

    for (int i = 0; i < count; i++) {
      if (i % 2 == 0) {
        assertEquals(Optional.empty(), btree.search(i));
      } else {
        assertEquals(Optional.of("value" + i), btree.search(i));
      }
    }
  }
}
