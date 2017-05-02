package com.cn.stormapplied.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.cn.stormapplied.services.C4_Order;
import com.cn.stormapplied.services.C4_NotificationService;

import java.util.Map;

public class C4_ProcessedOrderNotification {
    private C4_NotificationService notificationService;

    @Override
    declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output from this spout
    }

    @Override
    public void prepare(Map config, TopologyContext context) {
        this.notificationService = new C4_NotificationService();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        C4_Order order = (Order)tuple.getValueByField("order");
        notificationService.notifyOrderHasBeenProcessed("order");
    }
}
