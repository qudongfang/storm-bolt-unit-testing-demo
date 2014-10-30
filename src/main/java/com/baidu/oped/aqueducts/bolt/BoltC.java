/*
 * Copyright (C) 2014 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.oped.aqueducts.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * Author: qudongfang@baidu.com
 * Date: 2014:10:30 11:25:00
 */

public class BoltC extends BaseBasicBolt {
    private Producer<String, String> PRODUCER;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // this bolt do not emit any tuple
        // interact with db or other server
        String value = tuple.getStringByField("value");
        if (null == value || 0 == value.length()) {
            return;
        }

        value = value + ", processed by " + this.getClass().getName();

        this.PRODUCER.send(new KeyedMessage<String, String>("test", value));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // do not emit, not needed
    }
}
