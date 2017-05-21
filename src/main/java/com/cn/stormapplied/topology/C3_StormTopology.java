package com.cn.stormapplied.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

import com.cn.stormapplied.bolt.C3_HeatMapBuilderBolt;
import com.cn.stormapplied.spout.C3_CheckinsSpout;
import com.cn.stormapplied.bolt.C3_GeocodeLookupBolt;
import com.cn.stormapplied.bolt.C3_PersistorBolt;

public class C3_StormTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("checkins", new C3_CheckinsSpout());

        builder.setBolt("geocode-lookup", new C3_GeocodeLookupBolt())
                .shuffleGrouping("checkins");
        builder.setBolt("heatmap-builder", new C3_HeatMapBuilderBolt())
                .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3)
                .globalGrouping("geocode-lookup");
        builder.setBolt("persistor", new C3_PersistorBolt())
                .shuffleGrouping("heatmap-builder");

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("realtime-heatmap", config, topology);

        Utils.sleep(600000);
        cluster.killTopology("realtime-heatmap");
        cluster.shutdown();
    }
}
