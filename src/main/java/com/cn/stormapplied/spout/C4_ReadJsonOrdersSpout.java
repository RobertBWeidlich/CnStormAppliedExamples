package com.cn.stormapplied.spout;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.cn.stormapplied.services.C4_Order;

public class C4_ReadJsonOrdersSpout extends BaseRichSpout {
    private SpoutOutputCollector outputCollector;
    private List<C4_Order>       orders;

    private List<C4_Order> convertJsonList(List<String> jsonList) {
        List<C4_Order> orders = new ArrayList<C4_Order>(jsonList.size());
        Gson gson = new Gson();
        for (String json : jsonList) {
            C4_Order order = gson.fromJson(json, C4_Order.class);
            orders.add(order);
        }
        return orders;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("order"));
    }

    @Override
    public void open(Map                  map,
                     TopologyContext      topologyContext,
                     SpoutOutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        try {
            List<String> jsonList = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("C4_orders.txt"),
                    Charset.defaultCharset().name());
            this.orders = convertJsonList(jsonList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        for (C4_Order order : orders) {
            outputCollector.emit(new Values(order));
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }
}
