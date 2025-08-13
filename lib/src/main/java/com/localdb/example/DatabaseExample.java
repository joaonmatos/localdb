package com.localdb.example;

import com.localdb.LocalDatabaseImpl;
import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.nio.file.Paths;

public class DatabaseExample {
  public static void main(String[] args) {
    try {
      // Create a new database instance
      var db =
          LocalDatabaseImpl.create(
              Paths.get("example.db"),
              Paths.get("example.wal"),
              4, // B+ Tree order
              PrimitiveSerializationStrategy.STRING,
              PrimitiveSerializationStrategy.STRING,
              Comparator.STRING);

      // Basic operations
      db.put("name", "John Doe");
      db.put("age", "30");
      db.put("city", "San Francisco");

      System.out.println("Name: " + db.get("name").orElse("Not found"));
      System.out.println("Age: " + db.get("age").orElse("Not found"));

      // Transaction example
      var tx = db.beginTransaction();
      try {
        db.put("name", "Jane Smith", tx);
        db.put("occupation", "Engineer", tx);

        // Read within transaction
        System.out.println("Name in transaction: " + db.get("name", tx).orElse("Not found"));

        // Commit transaction
        db.commitTransaction(tx);

        System.out.println("Name after commit: " + db.get("name").orElse("Not found"));
      } catch (Exception e) {
        db.rollbackTransaction(tx);
        System.err.println("Transaction failed: " + e.getMessage());
      }

      // Range query example
      db.put("apple", "fruit");
      db.put("banana", "fruit");
      db.put("carrot", "vegetable");

      System.out.println("Items from 'a' to 'c': " + db.rangeQuery("a", "c"));

      // Clean up
      db.close();

    } catch (Exception e) {
      System.err.println("Database error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
