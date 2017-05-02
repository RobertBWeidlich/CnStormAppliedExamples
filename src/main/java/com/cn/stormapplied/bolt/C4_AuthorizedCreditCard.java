package com.cn.stormapplied.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cn.stormapplied.services.C4_Order;
import com.cn.stormapplied.services.C4_AuthorizationService;
import com.cn.stormapplied.services.C4_OrderDao;

import java.util.Map;

public class C4_AuthorizedCreditCard {
    private C4_AuthorizationService authorizationService;
    private C4_OrderDao             orderDao;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("order"));
    }

    @Override
    public void prepare(Map config, TopologyContext context) {
        this.orderDao = new C4_OrderDao();
        this.authorizationService = new C4_AuthorizationService();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        C4_Order order = (C4_Order)tuple.getValueByField("order");
        boolean isAuthorized = authorizationService.authorize(order);
        if (isAuthorized) {
            orderDao.updateStatusToDenied(order);
            orderDao.updateStatusToReadyToShip();
        }
        else {
            orderDao.updateStatusToDenied(order);
        }


    }


}
