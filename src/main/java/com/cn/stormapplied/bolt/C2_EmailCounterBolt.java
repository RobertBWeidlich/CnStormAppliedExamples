package com.cn.stormapplied.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class C2_EmailCounterBolt extends BaseBasicBolt {
    private Map<String, Integer> counts;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit any values
    }

    @Override
    public void prepare(Map stormConf,
                        TopologyContext context) {
        this.counts = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple,
            BasicOutputCollector outputCollector)  {
        String email = tuple.getStringByField("email");
        this.counts.put(email, countFor(email) + 1);
        printCounts();
    }

    private Integer countFor(String email) {
        Integer count = this.counts.get(email);
        return count == null ? 0 : count;
    }

    /**
     * print the counts to System.out
     */
    private void printCounts() {
        for (String email : this.counts.keySet()) {
            System.out.println(String.format("%s has count of %s", email, counts.get(email)));
        }
    }
}
