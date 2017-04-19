package com.cn.stormapplied.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

public class C3_LocalTopologyRunner {
    private static final int TEN_MINUTES = 600000;

    public static void main(String[] args) {
        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(C3_StormTopologyBuilder.TOPOLOGY_NAME, config, C3_StormTopologyBuilder.build());

        Utils.sleep(TEN_MINUTES);
        cluster.killTopology(C3_StormTopologyBuilder.TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
