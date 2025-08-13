package com.localdb.storage.btree;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExactCopyTest {

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
    System.out.println("Test starting - tree was created in @BeforeEach");
    System.out.println("About to call isEmpty()...");

    assertTrue(tree.isEmpty());
    System.out.println("isEmpty() completed successfully");

    System.out.println("About to call insert()...");
    tree.insert(1, "one");
    System.out.println("insert() completed successfully");

    var result = tree.search(1);
    assertTrue(result.isPresent());
  }
}
