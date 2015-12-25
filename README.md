# Distributed collections
##### _This is a field engineering repository for distributed collections implementation in XAP_
-----------------------------------------

## Building project

This project is based on Maven, so to build it, you would run next command:

```bash
# with unit tests
mvn clean package -Dcom.gs.home=<path to xap install>

# without unit tests
mvn clean package -DskipTests=true
```

Project contains multi-partitioned space unit tests, so a proper licence to run them is required. To pass the licence file to tests, the `com.gs.home` JVM argument is used. A proper value is path to folder without slash in the end, e.g. `/home/ec2-user/gigaspaces-xap-premium-10.2.0-ga`.

## Benchmarking the implementation

This project contains code for benchmarking the implementation using Java Microbenchmark Harness tool ([JMH](http://openjdk.java.net/projects/code-tools/jmh/)). The `collections-benchmark` module is packed into runnable `collections-benchmark/target/benchmarks.jar` when you build the project. With that, you can use the next command to run the benchmarking process:

```bash
java -jar benchmarks.jar -Dcom.gs.home=<path to xap install>
```

If you want to see runtime options for benchmarking, consider running the next command:

```bash
java -jar benchmarks.jar -h
```
