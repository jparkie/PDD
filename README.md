# ProbabilisticDeDuplicator (PDD)
[![Build Status](https://travis-ci.org/jparkie/PDD.svg?branch=master)](https://travis-ci.org/jparkie/PDD)
[![codecov](https://codecov.io/gh/jparkie/PDD/branch/master/graph/badge.svg)](https://codecov.io/gh/jparkie/PDD)

Implementation of *Advanced Bloom Filter Based Algorithms for Efficient Approximate Data De-Duplication in Streams* as described by Suman K. Bera, Sourav Dutta, Ankur Narang, and Souvik Bhattacherjee.

This library seeks to provide a production-oriented library for probabilistically de-duplicating unbounded data streams in real-time streaming scenarios (i.e. Storm, Spark, Flink, and Samza) while utilizing a fixed bound on memory.

Accordingly, this library implements three novel Bloom Filter algorithms from the prior-mentioned paper all of which are shown to converge faster towards stability and to improve false-negative rates (FNR) by 2 to 300 times in comparison with Stable Bloom Filters.

## Downloads

**Maven**
```
<dependency>
  <groupId>com.github.jparkie</groupId>
  <artifactId>pdd</artifactId>
  <version>0.1.1</version>
</dependency>

<dependency>
  <groupId>com.github.jparkie</groupId>
  <artifactId>pdd</artifactId>
  <version>0.1.2-SNAPSHOT</version>
</dependency>
```

**Gradle**
```
compile 'com.github.jparkie:pdd:0.1.1'

compile 'com.github.jparkie:pdd:0.1.2-SNAPSHOT'
```

## Usage

This library provides three implementations of a `ProbabilisticDeDuplicator`:

1. [Biased Sampling based Bloom Filter (BSBF)](https://github.com/jparkie/PDD/blob/master/src/main/java/com/github/jparkie/pdd/impl/BSBFDeDuplicator.java).

2. [Biased Sampling based Bloom Filter with Single Deletion (BSBFSD)](https://github.com/jparkie/PDD/blob/master/src/main/java/com/github/jparkie/pdd/impl/BSBFSDDeDuplicator.java).

3. [Randomized Load Balanced Biased Sampling based Bloom Filter (RLBSBF)](https://github.com/jparkie/PDD/blob/master/src/main/java/com/github/jparkie/pdd/impl/RLBSBFDeDuplicator.java).

### Basic

```java
final long NUM_BITS = 8 * 8L * 1024L * 1024L;
ProbabilisticDeDuplicator deDuplicator = null;

// Creates a BSBFDeDuplicator with 8MB of RAM and false-positive probability at 3%.
deDuplicator = RLBSBFDeDuplicator.create(NUM_BITS, 0.03D);

// Creates a BSBFDeDuplicator with 8MB of RAM and 5 hashing functions..
deDuplicator = new RLBSBFDeDuplicator(NUM_BITS, 5);

// The number of bits that the ProbabilisticDeDuplicator should use.
// Output: 67108864
System.out.println(deDuplicator.numBits());

// The number of hash functions that the ProbabilisticDeDuplicator should use.
// Output: 5
System.out.println(deDuplicator.numHashFunctions());

// Probabilistically classifies whether a given element is a distinct or a duplicate element.
// This operation does record the result into its history.
// Output: true
System.out.println(deDuplicator.classifyDistinct("Hello".getBytes()));
// Output: false
System.out.println(deDuplicator.classifyDistinct("Hello".getBytes()));

// Probabilistically peeks whether a given element is a distinct or a duplicate element.
// This operation does not record the result into its history.
// Output: true
System.out.println(deDuplicator.peekDistinct("World".getBytes()));
// Output: true
System.out.println(deDuplicator.peekDistinct("World".getBytes()));

// Version 0.1.2+: Calculate the probability that a distinct element of the stream is reported as duplicate.
// Output: Probability between 0 and 1.
System.out.println(deDuplicator.estimateFpp(actuallyDistinctProbability));
// Version 0.1.2+: Calculate the probability that a duplicate element of the stream is reported as distinct.
// Output: Probability between 0 and 1.
System.out.println(deDuplicator.estimateFnp(actuallyDistinctProbability));

// Reset the history of the ProbabilisticDeDuplicator.
deDuplicator.reset();
```

### Binary Serialization

PDD provides serializers for each `ProbabilisticDeDuplicator` implementation to write to and to read from a versioned binary format.

```java
// After Version 0.1.2:
// final ProbabilisticDeDuplicatorSerializer<RLBSBFDeDuplicator> serializer =
//                 RLBSBFDeDuplicatorSerializers.VERSION_2;

// Before Version 0.1.2:
final RLBSBFDeDuplicatorSerializer serializer = new RLBSBFDeDuplicatorSerializer();

final RLBSBFDeDuplicator deDuplicator = new RLBSBFDeDuplicator(64L, 1);
final Random random = new Random();
final byte[] element = new byte[128];
random.nextBytes(element);
assertTrue(deDuplicator.classifyDistinct(element));
final ByteArrayOutputStream out = new ByteArrayOutputStream();
serializer.writeTo(deDuplicator, out);
out.close();
final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
final RLBSBFDeDuplicator serialized = serializer.readFrom(in);
in.close();
assertEquals(deDuplicator, serialized);
```

### Java Serialization

PDD overrides the default object serialization for each `ProbabilisticDeDuplicator` implementation.

```java
final RLBSBFDeDuplicator deDuplicator = new RLBSBFDeDuplicator(64L, 1);
final Random random = new Random();
final byte[] element = new byte[128];
random.nextBytes(element);
assertTrue(deDuplicator.classifyDistinct(element));
final ByteArrayOutputStream out = new ByteArrayOutputStream();
final ObjectOutputStream oos = new ObjectOutputStream(out);
oos.writeObject(deDuplicator);
oos.close();
out.close();
final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
final ObjectInputStream ois = new ObjectInputStream(in);
final RLBSBFDeDuplicator serialized = (RLBSBFDeDuplicator) ois.readObject();
ois.close();
in.close();
assertEquals(deDuplicator, serialized);
```

## Build

```bash
$ git clone https://github.com/jparkie/PDD.git
$ cd PDD/
$ ./gradlew build
```

## References

- [Advanced Bloom Filter Based Algorithms for Efficient Approximate Data De-Duplication in Streams](https://arxiv.org/abs/1212.3964)

> Bera, S.K., Dutta, S., Narang, A., Bhattacherjee, S.: Advanced Bloom filter based algorithms for efficient approximate data de-duplication in streams (2012)
