package examples;

import com.localdb.storage.btree.PagedBPlusTree;
import com.localdb.storage.btree.BPlusTreeRebalancer;
import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Example demonstrating how to use the B+ tree rebalancer.
 */
public class RebalancerExample {
    
    public static void main(String[] args) throws Exception {
        Path dbFile = Paths.get("example_tree.db");
        
        try {
            // Create and populate a tree with some data
            System.out.println("Creating tree with sample data...");
            try (var tree = new PagedBPlusTree<>(
                    dbFile, 4, PrimitiveSerializationStrategy.INTEGER,
                    PrimitiveSerializationStrategy.STRING, Comparator.INTEGER, 100)) {
                
                // Insert data in a pattern that might create imbalance
                for (int i = 0; i < 50; i++) {
                    tree.insert(i, "value" + i);
                }
                
                // Delete some data to create potential imbalance
                for (int i = 10; i < 40; i += 3) {
                    tree.delete(i);
                }
                
                System.out.println("Inserted 50 values, deleted some to create imbalance");
            }

            // Get statistics before rebalancing
            BPlusTreeRebalancer<Integer, String> rebalancer = new BPlusTreeRebalancer<>(
                PrimitiveSerializationStrategy.INTEGER,
                PrimitiveSerializationStrategy.STRING,
                Comparator.INTEGER,
                4,
                100
            );
            
            System.out.println("\nTree statistics before rebalancing:");
            var beforeStats = rebalancer.getTreeStats(dbFile);
            System.out.println("  " + beforeStats);

            // Rebalance the tree
            System.out.println("\nRebalancing tree...");
            long startTime = System.currentTimeMillis();
            rebalancer.rebalance(dbFile);
            long endTime = System.currentTimeMillis();
            
            System.out.println("Rebalancing completed in " + (endTime - startTime) + " ms");

            // Get statistics after rebalancing
            System.out.println("\nTree statistics after rebalancing:");
            var afterStats = rebalancer.getTreeStats(dbFile);
            System.out.println("  " + afterStats);

            // Verify the tree still works correctly
            System.out.println("\nVerifying tree functionality after rebalancing...");
            try (var tree = new PagedBPlusTree<>(
                    dbFile, 4, PrimitiveSerializationStrategy.INTEGER,
                    PrimitiveSerializationStrategy.STRING, Comparator.INTEGER, 100)) {
                
                // Check some values
                System.out.println("Value for key 5: " + tree.search(5).orElse("NOT FOUND"));
                System.out.println("Value for key 15: " + tree.search(15).orElse("NOT FOUND")); // Should be deleted
                System.out.println("Value for key 25: " + tree.search(25).orElse("NOT FOUND"));
                
                // Get all pairs to verify order
                var allPairs = tree.getAllPairs();
                System.out.println("Total pairs in tree: " + allPairs.size());
                
                // Show first few pairs to verify order
                System.out.println("First 5 pairs:");
                for (int i = 0; i < Math.min(5, allPairs.size()); i++) {
                    var pair = allPairs.get(i);
                    System.out.println("  " + pair.getKey() + " -> " + pair.getValue());
                }
            }
            
        } finally {
            // Clean up
            Files.deleteIfExists(dbFile);
        }
    }
}
