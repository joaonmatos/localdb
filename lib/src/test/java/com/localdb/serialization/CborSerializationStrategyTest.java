package com.localdb.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.localdb.serialization.cbor.CborSerializationStrategy;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CborSerializationStrategyTest {

  static class TestClass {
    public String name;
    public int age;

    public TestClass() {}

    public TestClass(String name, int age) {
      this.name = name;
      this.age = age;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      TestClass testClass = (TestClass) obj;
      return age == testClass.age && name.equals(testClass.name);
    }
  }

  @Test
  void testObjectSerialization() throws IOException {
    CborSerializationStrategy<TestClass> strategy =
        new CborSerializationStrategy<>(TestClass.class);

    TestClass original = new TestClass("John Doe", 30);
    byte[] serialized = strategy.serialize(original);
    TestClass deserialized = strategy.deserialize(serialized);

    assertEquals(original.name, deserialized.name);
    assertEquals(original.age, deserialized.age);
  }

  @Test
  void testMapSerialization() throws IOException {
    var strategy = new CborSerializationStrategy<>(Map.class);

    var original = Map.of("key1", "value1", "key2", 42);
    var serialized = strategy.serialize(original);
    var deserialized = strategy.deserialize(serialized);

    assertEquals(original.get("key1"), deserialized.get("key1"));
    assertEquals(original.get("key2"), deserialized.get("key2"));
  }
}
