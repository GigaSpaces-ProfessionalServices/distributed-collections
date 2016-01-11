# Distributed collections
##### _This is a field engineering repository for distributed collections implementation in XAP_
-----------------------------------------

## Introduction

This project is a PoC for distributed java collections implementation on top of Gigaspaces XAP. It currently includes the implementation of `java.util.concurrent.BlockingQueue` and `java.util.Set` with a number of features on top of them, e.g. different collocation modes.

## Building project

This project is based on Maven, so to build it, you would run next command:

```bash
# with unit tests
mvn clean package -Dcom.gs.home=<path to xap install>

# without unit tests
mvn clean package -DskipTests=true
```

Project contains multi-partitioned space unit tests, so a proper licence to run them is required. To pass the licence file to tests, the `com.gs.home` JVM argument is used. A proper value is path to folder without slash in the end, e.g. `/home/ec2-user/gigaspaces-xap-premium-10.2.0-ga`.

You can also find build configuration and artifacts for this project at our [Teamcity CI/CD server](http://10.8.1.76:8111/overview.html) on AWS.

## Using collections

Each collection declaration requires passing `Gigaspace` to the configurer or bean factory. Here is how you can create set and queue via Spring XML and Java configurations:

### Declaring `GigaSet`

##### Java declaration

```java
GigaSet<Person> gigaSet = new GigaSetConfigurer<Person>(gigaSpace).gigaSet();
```

##### XML declaration

```xml
<!-- Space declaration is omitted -->

<os-core:giga-space id="myGigaSpace" space="space"/>

<bean id="myGigaSet" class="org.openspaces.collections.GigaSetFactoryBean">
  <property name="gigaSpace" ref="myGigaSpace"/>
</bean>
```

```java
@Service
public class MyService {
    @Resource(name = "myGigaSet")
    private GigaSet<SerializableType> set;
}
```

### Declaring `GigaBlockingQueue`

##### Java declaration

```java
GigaQueue<Person> queue = new GigaQueueConfigurer<Person>(gigaSpace, "myPersonQueue", CollocationMode.DISTRIBUTED).elementType(Person.class).gigaQueue();
```

##### XML declaration

```xml
<!-- Space declaration is omitted -->

<os-core:giga-space id="myGigaSpace" space="space"/>

<bean id="myGigaQueue" class="org.openspaces.collections.GigaQueueFactoryBean">
  <property name="queueName" value="myQueue"/>
  <property name="gigaSpace" ref="myGigaSpace"/>
  <property name="collocationMode" value="DISTRIBUTED"/>
</bean>
```

```java
@Service
public class MyService {
    @Resource(name = "myGigaQueue")
    private GigaBlockingQueue<SerializableType> queue;
}
```

## Benchmarking the implementation

This project contains code for benchmarking the implementation using Java Microbenchmark Harness tool ([JMH](http://openjdk.java.net/projects/code-tools/jmh/)). The `collections-benchmark` module is packed into runnable `collections-benchmark/target/benchmarks.jar` when you build the project. With that, you can use the next command to run the benchmarking process:

```bash
java -jar -Dcom.gs.home=<path to xap install> benchmarks.jar
```

If you want to see runtime options for benchmarking, consider running the next command:

```bash
java -jar benchmarks.jar -h
```
