package com.localdb.storage.btree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SimplePagedBPlusTreeTest {

  @TempDir Path tempDir;

  private Path dbFile;
  private PagedBPlusTree<Integer, String> tree;

  @BeforeEach
  void setUp() throws IOException {
    dbFile = tempDir.resolve("simple.db");
    tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            10 // Very small buffer pool
            );
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tree != null) {
      tree.close();
    }
  }

  @Test
  void testBasicFunctionality() throws IOException {
    assertTrue(tree.isEmpty());

    tree.insert(1, "one");

    assertEquals(java.util.Optional.of("one"), tree.search(1));
  }
}
