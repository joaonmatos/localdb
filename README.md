# LocalDB - Java Database Implementation

A comprehensive Java database implementation using B+ Trees with transaction support, Write-Ahead Logging (WAL), and pluggable serialization strategies.

This project was developed solely using Claude Code.

## Features

- **B+ Tree Storage**: Efficient key-value storage using B+ trees for fast queries and range operations
- **ACID Transactions**: Multi-key transaction support with rollback capabilities
- **Write-Ahead Logging**: Durability guarantees through WAL with crash recovery
- **Pluggable Serialization**: Strategy pattern for keys and values with built-in support for primitives and CBOR
- **Range Queries**: Efficient range scans leveraging B+ tree structure
- **Thread Safety**: Concurrent access support with read-write locks

## Architecture

### Core Components

1. **Serialization Layer** (`com.localdb.serialization`)
   - `SerializationStrategy<T>`: Interface for type-specific serialization
   - `PrimitiveSerializationStrategy`: Binary encodings for primitives (int, long, string, double, byte[])
   - `CborSerializationStrategy<T>`: CBOR serialization for complex objects

2. **Storage Layer** (`com.localdb.storage.btree`)
   - `BPlusTree<K,V>`: Main B+ tree implementation
   - `BPlusTreeNode<K,V>`: Abstract node base class
   - `BPlusTreeLeafNode<K,V>`: Leaf node implementation
   - `BPlusTreeInternalNode<K,V>`: Internal node implementation

3. **Transaction Layer** (`com.localdb.transaction`)
   - `Transaction<K,V>`: Individual transaction context
   - `TransactionManager<K,V>`: Transaction lifecycle management
   - `TransactionState`: Transaction state enumeration

4. **WAL Layer** (`com.localdb.wal`)
   - `WriteAheadLog<K,V>`: WAL interface
   - `FileWriteAheadLog<K,V>`: File-based WAL implementation
   - `WALEntry<K,V>`: Individual log entry

5. **Database Interface** (`com.localdb`)
   - `LocalDatabase<K,V>`: Main database interface
   - `LocalDatabaseImpl<K,V>`: Primary implementation

## Quick Start

### Dependencies

```kotlin
dependencies {
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.18.2")
    implementation("com.fasterxml.jackson.core:jackson-core:2.18.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("ch.qos.logback:logback-classic:1.5.12")
    implementation("com.google.guava:guava:33.4.6-jre")
}
```

### Basic Usage

```java
import com.localdb.LocalDatabase;
import com.localdb.LocalDatabaseImpl;
import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import com.localdb.transaction.Transaction;
import java.nio.file.Paths;

// Create database
LocalDatabase<String, String> db = LocalDatabaseImpl.create(
    Paths.get("data.db"),           // Data file path
    Paths.get("wal.log"),           // WAL file path  
    4,                              // B+ tree order
    PrimitiveSerializationStrategy.STRING,  // Key serializer
    PrimitiveSerializationStrategy.STRING,  // Value serializer
    Comparator.STRING               // Key comparator
);

// Basic operations
db.put("key1", "value1");
Optional<String> value = db.get("key1");
db.delete("key1");

// Transactions
Transaction<String, String> tx = db.beginTransaction();
try {
    db.put("key2", "value2", tx);
    db.put("key3", "value3", tx);
    db.commitTransaction(tx);
} catch (Exception e) {
    db.rollbackTransaction(tx);
}

// Range queries
List<String> values = db.rangeQuery("key1", "key9");

// Cleanup
db.close();
```

### Serialization Strategies

#### Built-in Primitive Types

```java
// Integers
PrimitiveSerializationStrategy.INTEGER
PrimitiveSerializationStrategy.LONG

// Strings  
PrimitiveSerializationStrategy.STRING

// Floating point
PrimitiveSerializationStrategy.DOUBLE

// Binary data
PrimitiveSerializationStrategy.BYTE_ARRAY
```

#### Complex Objects with CBOR

```java
// For custom objects
CborSerializationStrategy<MyClass> strategy = 
    new CborSerializationStrategy<>(MyClass.class);

LocalDatabase<String, MyClass> db = LocalDatabaseImpl.create(
    dataPath, walPath, order,
    PrimitiveSerializationStrategy.STRING,
    strategy,
    Comparator.STRING
);
```

## Building

```bash
./gradlew build
```

## Testing

```bash
./gradlew test
```

## Design Decisions

### B+ Tree Implementation

- **Order**: Configurable order for tuning performance vs. space
- **Leaf Node Linking**: Sequential access through leaf node pointers
- **Split Strategy**: Even splits for balanced tree structure

### Transaction Isolation

- **Read Committed**: Transactions see committed changes and their own uncommitted changes
- **Write Locking**: Global write locks prevent concurrent modifications
- **WAL Ordering**: Sequential numbering ensures consistent replay

### Durability

- **Synchronous WAL**: All operations logged before acknowledgment
- **Recovery**: Automatic recovery on startup replays committed transactions
- **Checkpointing**: Periodic compaction of WAL entries

## Limitations

- **Single-node**: Not designed for distributed deployments
- **Memory Usage**: Entire B+ tree structure kept in memory
- **Concurrency**: Write operations are globally serialized
- **Storage Format**: Custom binary format, not portable

## License

This project is for educational purposes. See LICENSE file for details.
