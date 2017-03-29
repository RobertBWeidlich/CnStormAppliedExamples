package com.cn.stormapplied.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * This spout simulates reading commits from a live stream by doing two things:
 *   1. Reading a file containing commit data into a list of strings (one string per commit)
 *   2. When nextTuple() is called, emit a tuple for each string in the list.
 */
public class C2_CommitFeedListenerSpout extends BaseRichSpout {
    private SpoutOutputCollector outputCollector;
    private List<String>         commits;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("commit"));
    }

    @Override
    public void open(Map map,
                     TopologyContext context,
                     SpoutOutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        try {
            ClassLoader loader = C2_CommitFeedListenerSpout.class.getClassLoader();
            java.net.URL url = loader.getResource("");
            //java.net.URL url1 = ClassLoader.getSystemClassLoader("");
            commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("changelog.txt"),
                    Charset.defaultCharset().name());
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        for (String commit : this.commits) {
            outputCollector.emit(new Values(commit));
        }
    }
}
