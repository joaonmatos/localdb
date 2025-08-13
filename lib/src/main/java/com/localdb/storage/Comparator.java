package com.localdb.storage;

/**
 * A functional interface for comparing keys in the database storage layer. This interface is used
 * by B+ tree implementations to maintain proper ordering.
 *
 * <p>Implementations must provide a consistent ordering that satisfies the properties of a total
 * ordering: reflexive, antisymmetric, and transitive.
 *
 * @param <T> the type of objects that may be compared by this comparator
 */
public interface Comparator<T> {
  /**
   * Compares two objects for order.
   *
   * @param a the first object to be compared
   * @param b the second object to be compared
   * @return a negative integer, zero, or a positive integer as the first argument is less than,
   *     equal to, or greater than the second
   */
  int compare(T a, T b);

  /** A comparator for Integer values using natural ordering. */
  static Comparator<Integer> INTEGER = Integer::compareTo;

  /** A comparator for Long values using natural ordering. */
  static Comparator<Long> LONG = Long::compareTo;

  /** A comparator for String values using lexicographic ordering. */
  static Comparator<String> STRING = String::compareTo;

  /** A comparator for Double values using natural ordering. */
  static Comparator<Double> DOUBLE = Double::compareTo;

  /**
   * Returns a comparator that compares {@link Comparable} objects in natural order.
   *
   * @param <T> the type of objects that may be compared by this comparator
   * @return a comparator that uses natural ordering
   */
  static <T extends Comparable<T>> Comparator<T> natural() {
    return (a, b) -> a.compareTo(b);
  }
}
