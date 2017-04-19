package com.cn.stormapplied.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.cn.stormapplied.spout.C2_CommitFeedListenerSpout;
import com.cn.stormapplied.bolt.C2_EmailExtractorBolt;
import com.cn.stormapplied.bolt.C2_EmailCounterBolt;


public class C2_CountCommitsTopology{
    private static final int TEN_MINUTES = 600000;
    private static final int ONE_MINUTE  = 60000;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", new C2_CommitFeedListenerSpout());

        builder.setBolt("email-extractor", new C2_EmailExtractorBolt())
               .shuffleGrouping("commit-feed-listener");

        builder.setBolt("email-counter", new C2_EmailCounterBolt())
               .fieldsGrouping("email-extractor", new Fields("email"));

        Config config = new Config();
        config.setDebug(true);
        //config.setDebug(false);

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("c2-count-commits-topology", config, topology);

        Utils.sleep(ONE_MINUTE);
        cluster.killTopology("c2-count-commits-topology");
        cluster.shutdown();
    }
}
