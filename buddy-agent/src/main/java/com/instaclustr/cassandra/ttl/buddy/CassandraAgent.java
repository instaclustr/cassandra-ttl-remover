package com.instaclustr.cassandra.ttl.buddy;

import java.lang.instrument.Instrumentation;

import net.bytebuddy.agent.builder.AgentBuilder.Default;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.cassandra.config.Config.CorruptedTombstoneStrategy;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.io.util.SpinningDiskOptimizationStrategy;
import org.apache.cassandra.metrics.RestorableMeter;

public class CassandraAgent {

    public static void premain(String arg, Instrumentation inst) {

        final Default agentBuilder = new Default();

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getBufferPoolUseHeapIfExhausted")).intercept(FixedValue.value(true)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.db.SystemKeyspace"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getSSTableReadMeter")).intercept(FixedValue.value(new RestorableMeter())))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("shouldMigrateKeycacheOnCompaction")).intercept(FixedValue.value(true)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getCompactionLargePartitionWarningThreshold")).intercept(FixedValue.value(100 * 1024 * 1024)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getColumnIndexSize")).intercept(FixedValue.value(64 * 1024)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getColumnIndexCacheSize")).intercept(FixedValue.value(2 * 1024)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getCorruptedTombstoneStrategy")).intercept(FixedValue.value(CorruptedTombstoneStrategy.warn)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getMaxValueSize")).intercept(FixedValue.value(256 * 1024 * 1024)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getTrickleFsyncIntervalInKb")).intercept(FixedValue.value(10240)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getTrickleFsync")).intercept(FixedValue.value(false)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getDiskAccessMode")).intercept(FixedValue.value(DiskAccessMode.standard)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getSSTablePreemptiveOpenIntervalInMB")).intercept(FixedValue.value(50)))
            .installOn(inst);

        // on 3.11.x, there is typo - check "Preempive" in that method
        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getSSTablePreempiveOpenIntervalInMB")).intercept(FixedValue.value(50)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getDiskOptimizationEstimatePercentile")).intercept(FixedValue.value(0.95)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getFileCacheRoundUp")).intercept(FixedValue.value(false)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getAllDataFileLocations")).intercept(FixedValue.value(new String[]{})))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getFileCacheSizeInMB")).intercept(FixedValue.value(1)))
            .installOn(inst);

        agentBuilder
            .type(ElementMatchers.named("org.apache.cassandra.config.DatabaseDescriptor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                           builder.method(ElementMatchers.named("getDiskOptimizationStrategy")).intercept(FixedValue.value(new SpinningDiskOptimizationStrategy())))
            .installOn(inst);
    }
}
