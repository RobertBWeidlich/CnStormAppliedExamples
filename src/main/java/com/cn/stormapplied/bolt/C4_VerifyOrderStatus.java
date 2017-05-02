package com.cn.stormapplied.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cn.stormapplied.services.C4_Order;
import com.cn.stormapplied.services.C4_OrderDao;

import java.util.Map;

public class C4_VerifyOrderStatus {
    private C4_OrderDao orderDao;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("order"));
    }

    @Override
    public void prepare(Map config, TopologyContext context) {
        this.orderDao = new OrderDao;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        C4_Order order = (Order)tuple.getValueByField("order");
        if (this.orderDao.isNotReadyToShip(order)) {
            outputCollector.emit(new Values(order));
        }
    }
}
