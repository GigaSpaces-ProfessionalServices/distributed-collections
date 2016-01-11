# Distributed collections
##### _This is a field engineering repository for distributed collections implementation in XAP_
-----------------------------------------

[_Introduction_](#introduction)

[1. Building project](#building-project)

[2. Using collections](#using-collections)

[3. Configuring GigaSet](#configuring-gigaset)

[4. Configuring GigaQueue](#configuring-gigaqueue)

[5. Benchmarking the implementation](#benchmarking-the-implementation)

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

##### Java declaration

```java
GigaSet<Person> set = new GigaSetConfigurer<Person>(gigaSpace).gigaSet();
GigaQueue<Person> queue = new GigaQueueConfigurer<Person>(gigaSpace, "myPersonQueue", CollocationMode.DISTRIBUTED).gigaQueue();
```

##### XML declaration

```xml
<!-- Space declaration is omitted -->

<os-core:giga-space id="myGigaSpace" space="space"/>

<bean id="myGigaSet" class="org.openspaces.collections.GigaSetFactoryBean">
  <property name="gigaSpace" ref="myGigaSpace"/>
</bean>

<bean id="myGigaQueue" class="org.openspaces.collections.GigaQueueFactoryBean">
  <property name="queueName" value="myQueue"/>
  <property name="gigaSpace" ref="myGigaSpace"/>
  <property name="collocationMode" value="DISTRIBUTED"/>
</bean>
```

```java
@Service
public class MyService {
    @Resource(name = "myGigaSet")
    private GigaSet<SerializableType> set;

    @Resource(name = "myGigaQueue")
    private GigaBlockingQueue<SerializableType> queue;
}
```

> Only `Serializable` elements can be stored in `GigaSet`


## Configuring GigaSet

Current implementation of `GigaSet` supports two modes: `clustered` and `non-clustered`. If a specific mode is not chosen, it will be set automatically based on given `GigaSpace`. Clustered mode is set to `true` if the space is an embedded one and the space is not a local cache proxy, `false` otherwise. When declaring the mode, user can leave this flag unset or choose `true` to work with single member of the grid or `false` to work with the whole cluster. This allows to switch between client-side implementation of `GigaSet` which interacts with remote space and server-side implementation that works with own embedded space.

Here is an example of how to create a `GigaSet` for a single grid member (embedded space):

```java
GigaSet<Person> set = new GigaSetConfigurer<Person>(gigaSpace)
                        .clustered(clustered)
                        .gigaSet();
```

Or, with XML configuration:

```xml
<!-- Embedded space declaration is omitted -->

<os-core:giga-space id="myGigaSpace" space="space"/>

<bean id="myGigaSet" class="org.openspaces.collections.GigaSetFactoryBean">
  <property name="gigaSpace" ref="myGigaSpace"/>
  <property name="clustered" value="true"/>
</bean>
```

## Configuring GigaQueue

`GigaQueue` supports three collocation modes, multi-client usage, non-serializable element types and capacity limits.

#### Distributed collocation mode

![Distributed collocation mode](./docs/distributed.png?raw=true)

In this mode collection items are spread over the whole cluster. It is preferred mode for collections of huge sizes, since memory consumption is balanced across the grid. User items are wrapped into meta classes, but can still be queried (only serializable items).

#### Local collocation mode

![Local collocation mode](./docs/local.png?raw=true)

In `local` collocation mode items are stored within the same partition as the metadata object. This mode should be chosen when application interacts with multiple small collections stored in the grid. It is scalable by collection count and not by the number of items in one collection. User items are wrapped into meta classes, but can still be queried (only serializable items).

#### Embedded collocation mode

![Embedded collocation mode](./docs/embedded.png?raw=true)

This mode suggests that user items are stored inside single collection container object. Thus items do not have their own space identity and cannot be queried. This mode is similar to `local` mode but does not expose collection items for direct external use.

#### Multi-client usage

![Multi-client usage](./docs/multi-client.png?raw=true)

`GigaQueue` can have multiple clients operating over one collection. Several processes may declare a queue with one name: this will create only one queue structure in space and will allow clients to offer and poll with a single source. For example, queue may be filled with tasks by the manager process and emptied by the workers.

#### Serialization

By default all queue items will be serialized into byte arrays and stored in space, which brings inability to query the items directly. To tweak the serialization, you can provide an item class which will be used to determine if items must be stored in byte form or note. Next configuration will skip additional serialization, if `Person` class implements `Serializable`:

```java
GigaQueue<Person> queue = new GigaQueueConfigurer<Person>(gigaSpace, "myPersonQueue", DISTRIBUTED)
                            .elementType(Person.class)
                            .gigaQueue();
```

Or, with XML configuration:

```xml
<bean id="myGigaQueue" class="org.openspaces.collections.GigaQueueFactoryBean">
  <property name="queueName" value="myQueue"/>
  <property name="gigaSpace" ref="myGigaSpace"/>
  <property name="collocationMode" value="DISTRIBUTED"/>
  <property name="elementType" value="com.myproject.bean.Person"/>
</bean>
```

You can also provide custom serializers and serializer providers using `GigaQueueFactoryBean`.

#### Capacity limits

`GigaQueue` can be limited on capacity. This will change queue behavior when items count reaches the maximum. More information about bounded queue behavior can be found in method docs. To create a bounded queue, next declaration may be used:

```java
GigaQueue<Person> queue = new GigaQueueConfigurer<Person>(gigaSpace, "myPersonQueue", DISTRIBUTED)
                            .capacity(100)
                            .gigaQueue();
```

Or, with XML configuration:

```xml
<bean id="myGigaQueue" class="org.openspaces.collections.GigaQueueFactoryBean">
  <property name="queueName" value="myQueue"/>
  <property name="gigaSpace" ref="myGigaSpace"/>
  <property name="collocationMode" value="DISTRIBUTED"/>
  <property name="capacity" value="100"/>
</bean>
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
