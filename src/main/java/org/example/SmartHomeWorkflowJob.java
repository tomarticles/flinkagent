package org.example;

import org.apache.flink.agents.api.AgentsExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class SmartHomeWorkflowJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        AgentsExecutionEnvironment agentsEnv = AgentsExecutionEnvironment.getExecutionEnvironment(env);

        // Demo sensor snapshots
        DataStream<String> snapshots = env.fromCollection(List.of(
                "{\"homeId\":\"h1\",\"ts\":\"2026-02-15T20:00:00Z\",\"tempC\":22.3,\"humidityPct\":42,\"co2ppm\":820,\"powerW\":420}",
                "{\"homeId\":\"h1\",\"ts\":\"2026-02-15T20:05:00Z\",\"tempC\":29.4,\"humidityPct\":67,\"co2ppm\":1650,\"powerW\":780}",
                "{\"homeId\":\"h2\",\"ts\":\"2026-02-15T20:10:00Z\",\"tempC\":24.0,\"humidityPct\":45,\"co2ppm\":950,\"powerW\":4100}"
        ));

        DataStream<Object> results =
                agentsEnv
                        .fromDataStream(snapshots)
                        .apply(new SmartHomeKpiAgent())
                        .toDataStream();

        results.print();

        agentsEnv.execute();
    }
}