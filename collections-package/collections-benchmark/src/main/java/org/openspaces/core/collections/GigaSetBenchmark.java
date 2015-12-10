package org.openspaces.core.collections;

import java.util.HashSet;
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
    private static final int ELEMENTS_COUNT = 100;
    
    @Param({ "0", "1", "1000"})
    int size;
    
    AbstractApplicationContext applicationContext;
    GigaSet<ComplexType> gigaSet;
    
    ComplexType complexType;
    Set<ComplexType> complexTypes;

    @Setup(Level.Trial)
    public void init() {
        applicationContext = new ClassPathXmlApplicationContext(PARTITIONED_SPACE_CONFIG);
        gigaSet = applicationContext.getBean(GigaSet.class);
        complexType = create();
        complexTypes = new HashSet<>();
        populate(ELEMENTS_COUNT, complexTypes);
        
    }
    
    @Setup(Level.Invocation)
    public void setup() {
        gigaSet.clear();
        populate(size, gigaSet);
    }
    
    @TearDown(Level.Trial)
    public void destroy() {
        applicationContext.close();
    }
    
    private void populate(int size, Set<ComplexType> set) {
        for (int i = 0; i < size; i++) {
            set.add(create());
        }
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
        return gigaSet.add(complexType);
    }
    
    @Benchmark
    public boolean testAddAll() {
        return gigaSet.addAll(complexTypes);
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
