package com.localdb.tools;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import com.localdb.storage.btree.BPlusTreeRebalancer;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Command-line tool for rebalancing B+ tree files.
 *
 * <p>Usage: java com.localdb.tools.TreeRebalanceTool <tree-file-path> [options]
 *
 * <p>Options: --order <n> B+ tree order (default: 4) --buffer-size <n> Buffer pool size (default:
 * 1000) --key-type <type> Key type: INTEGER, LONG, STRING, DOUBLE (default: INTEGER) --value-type
 * <type> Value type: INTEGER, LONG, STRING, DOUBLE (default: STRING) --stats Show tree statistics
 * before and after rebalancing --help Show this help message
 */
public class TreeRebalanceTool {

  public static void main(String[] args) {
    if (args.length == 0 || "--help".equals(args[0])) {
      showHelp();
      return;
    }

    try {
      Config config = parseArgs(args);
      rebalanceTree(config);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      System.exit(1);
    }
  }

  private static void showHelp() {
    System.out.println("TreeRebalanceTool - Rebalances B+ tree files offline");
    System.out.println();
    System.out.println(
        "Usage: java com.localdb.tools.TreeRebalanceTool <tree-file-path> [options]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --order <n>           B+ tree order (default: 4)");
    System.out.println("  --buffer-size <n>     Buffer pool size (default: 1000)");
    System.out.println(
        "  --key-type <type>     Key type: INTEGER, LONG, STRING, DOUBLE (default: INTEGER)");
    System.out.println(
        "  --value-type <type>   Value type: INTEGER, LONG, STRING, DOUBLE (default: STRING)");
    System.out.println("  --stats               Show tree statistics before and after rebalancing");
    System.out.println("  --help                Show this help message");
    System.out.println();
    System.out.println("Examples:");
    System.out.println("  java com.localdb.tools.TreeRebalanceTool /path/to/tree.db");
    System.out.println(
        "  java com.localdb.tools.TreeRebalanceTool /path/to/tree.db --order 8 --stats");
    System.out.println(
        "  java com.localdb.tools.TreeRebalanceTool /path/to/tree.db --key-type STRING --value-type STRING");
  }

  private static Config parseArgs(String[] args) {
    Config config = new Config();
    config.treePath = Paths.get(args[0]);

    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--order":
          if (i + 1 >= args.length) throw new IllegalArgumentException("--order requires a value");
          config.order = Integer.parseInt(args[++i]);
          break;
        case "--buffer-size":
          if (i + 1 >= args.length)
            throw new IllegalArgumentException("--buffer-size requires a value");
          config.bufferSize = Integer.parseInt(args[++i]);
          break;
        case "--key-type":
          if (i + 1 >= args.length)
            throw new IllegalArgumentException("--key-type requires a value");
          config.keyType = args[++i];
          break;
        case "--value-type":
          if (i + 1 >= args.length)
            throw new IllegalArgumentException("--value-type requires a value");
          config.valueType = args[++i];
          break;
        case "--stats":
          config.showStats = true;
          break;
        default:
          throw new IllegalArgumentException("Unknown option: " + args[i]);
      }
    }

    return config;
  }

  private static void rebalanceTree(Config config) throws Exception {
    System.out.println("Rebalancing tree: " + config.treePath);
    System.out.println("Configuration:");
    System.out.println("  Order: " + config.order);
    System.out.println("  Buffer size: " + config.bufferSize);
    System.out.println("  Key type: " + config.keyType);
    System.out.println("  Value type: " + config.valueType);
    System.out.println();

    // Create rebalancer based on key/value types
    // For simplicity, we'll support the most common case: Integer keys, String values
    if ("INTEGER".equals(config.keyType) && "STRING".equals(config.valueType)) {
      BPlusTreeRebalancer<Integer, String> rebalancer =
          new BPlusTreeRebalancer<>(
              PrimitiveSerializationStrategy.INTEGER,
              PrimitiveSerializationStrategy.STRING,
              Comparator.INTEGER,
              config.order,
              config.bufferSize);

      if (config.showStats) {
        System.out.println("Tree statistics before rebalancing:");
        var beforeStats = rebalancer.getTreeStats(config.treePath);
        System.out.println("  " + beforeStats);
        System.out.println();
      }

      long startTime = System.currentTimeMillis();
      rebalancer.rebalance(config.treePath);
      long endTime = System.currentTimeMillis();

      System.out.println("Rebalancing completed in " + (endTime - startTime) + " ms");

      if (config.showStats) {
        System.out.println();
        System.out.println("Tree statistics after rebalancing:");
        var afterStats = rebalancer.getTreeStats(config.treePath);
        System.out.println("  " + afterStats);
      }

    } else {
      throw new UnsupportedOperationException(
          "Key type "
              + config.keyType
              + " and value type "
              + config.valueType
              + " combination not yet supported. Currently only INTEGER keys with STRING values are supported.");
    }
  }

  private static class Config {
    Path treePath;
    int order = 4;
    int bufferSize = 1000;
    String keyType = "INTEGER";
    String valueType = "STRING";
    boolean showStats = false;
  }
}
