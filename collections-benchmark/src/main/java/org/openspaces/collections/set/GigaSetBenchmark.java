package org.openspaces.collections.set;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@SuppressWarnings("unchecked")
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class GigaSetBenchmark {

    private static final String PARTITIONED_SPACE_CONFIG = "partitioned-space-test-config.xml";
    private static final int ELEMENTS_MAX_COUNT = 100;
    
    @Param({ "0", "1", "1000"})
    private int size;
    
    private AbstractApplicationContext applicationContext;
    private GigaSet<ComplexType> gigaSet;
    
    private ComplexType newComplexType;
    private Set<ComplexType> newComplexTypes;
    
    private ComplexType existingComplexType;
    private Set<ComplexType> existingComplexTypes;

    @Setup(Level.Trial)
    public void init() {
        applicationContext = new ClassPathXmlApplicationContext(PARTITIONED_SPACE_CONFIG);
        gigaSet = applicationContext.getBean(GigaSet.class);
        
        newComplexType = create();
        newComplexTypes = new HashSet<>();
        for (int i = 0; i < ELEMENTS_MAX_COUNT; i++) {
            newComplexTypes.add(create());
        }
    }
    
    @Setup(Level.Invocation)
    public void setup() {
        gigaSet.clear();
        
        existingComplexTypes = new HashSet<>();
       
        //if size is 0 then any not null value
        if (size == 0) {
            existingComplexType = create();
        }
        
        for (int i = 0; i < size; i++) {
            ComplexType complexType = create();
            if (i == 0) {
                existingComplexType = complexType;
            }
        
            if (i < ELEMENTS_MAX_COUNT) {
                existingComplexTypes.add(complexType);
            }
            gigaSet.add(complexType);
        }
    }
    
    @TearDown(Level.Trial)
    public void destroy() {
        applicationContext.close();
    }
    
    private ComplexType create() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        Long id = random.nextLong();
        return new ComplexTypeBuilder(id)
                .setNumber(random.nextLong())
                .setDescription("Foo" + id)
                .addChild(random.nextLong())
                .addChild(random.nextLong())
                .build();
    }
    
    @Benchmark
    public boolean testAdd() {
        return gigaSet.add(newComplexType);
    }
    
    @Benchmark
    public boolean testAddAll() {
        return gigaSet.addAll(newComplexTypes);
    }
    
    @Benchmark
    public boolean testRemove() {
        return gigaSet.remove(existingComplexType);
    }
    
    @Benchmark
    public boolean testRemoveAll() {
        return gigaSet.removeAll(existingComplexTypes);
    }
    
    @Benchmark
    public boolean testContains() {
        return gigaSet.contains(existingComplexType);
    }
    
    @Benchmark
    public boolean testContainsAll() {
        return gigaSet.containsAll(existingComplexTypes);
    }
    
    @Benchmark
    public boolean testRetainAll() {
        return gigaSet.retainAll(existingComplexTypes);
    }
    
    @Benchmark
    public void testClear() {
        gigaSet.clear();
    }
    
    @Benchmark
    public void testIterator(Blackhole bh) {
        for (Iterator<ComplexType> iterator = gigaSet.iterator(); iterator.hasNext();) {
            bh.consume(iterator.next());
        }
    }
    
    @Benchmark
    public ComplexType[] testToArray() {
        return gigaSet.toArray(new ComplexType[size]);
    }
    
    @Benchmark
    public int testSize() {
        return gigaSet.size();
    }
    
    @Benchmark
    public boolean isEmpty() {
        return gigaSet.isEmpty();
    }
    
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(GigaSetBenchmark.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .threads(1)
            .build();

        new Runner(opt).run();
    }
}
