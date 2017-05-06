package com.cn.stormapplied.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.cn.stormapplied.bolt.C4_AuthorizedCreditCardBolt;
import com.cn.stormapplied.bolt.C4_ProcessedOrderNotificationBolt;
import com.cn.stormapplied.bolt.C4_VerifyOrderStatusBolt;
import com.cn.stormapplied.spout.C4_ReadJsonOrdersSpout;

public class C4_CreditCardAuths {
    private static final String TOPOLOGY_ID = "c4_credit_card";
    private static final String SPOUT1_ID =   "c4_read_json_orders";
    private static final String BOLT1_ID =    "c4_verify_order_status";
    private static final String BOLT2_ID =    "c4_authorized_credit_card";
    private static final String BOLT3_ID =    "c4_processed_order_notification";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT1_ID, new C4_ReadJsonOrdersSpout());
        builder.setBolt(BOLT1_ID, new C4_VerifyOrderStatusBolt()).shuffleGrouping(SPOUT1_ID);
        builder.setBolt(BOLT2_ID, new C4_AuthorizedCreditCardBolt()).shuffleGrouping(BOLT1_ID);
        builder.setBolt(BOLT3_ID, new C4_ProcessedOrderNotificationBolt()).shuffleGrouping(BOLT2_ID);

        Config config = new Config();
        config.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(TOPOLOGY_ID, config, builder.createTopology());

        Utils.sleep(600000);
        localCluster.shutdown();
    }
}
