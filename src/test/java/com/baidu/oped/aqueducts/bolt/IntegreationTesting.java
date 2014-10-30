/*
 * Copyright (C) 2014 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.oped.aqueducts.bolt;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import junit.framework.TestCase;
import kafka.producer.KeyedMessage;

/**
 * Author: qudongfang@baidu.com
 * Date: 2014:10:30 15:11:00
 */

public class IntegreationTesting extends TestCase {
    public void testExecute() throws Exception {

        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster iLocalCluster) throws Exception {
                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();

                FeederSpout feeder = new FeederSpout(new Fields("value"));
                builder.setSpout("feederSpout", feeder);

                // set some test bolt
                builder.setBolt("boltA", new BoltA(), 1).shuffleGrouping("feederSpout");
                builder.setBolt("boltB", new BoltB(), 1).shuffleGrouping("boltA");
                /*
                BoltC boltC = new BoltC();
                Producer<String, String> PRODUCER = mock(Producer.class);
                Field privateField = BoltC.class.getDeclaredField("PRODUCER");
                privateField.setAccessible(true);
                privateField.set(boltC, PRODUCER);
                builder.setBolt("boltC", boltC, 1)
                    .shuffleGrouping("boltB");
                */

                StormTopology topology = builder.createTopology();
                // complete the topology

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                String input = "input";
                mockedSources.addMockData("feederSpout", new Values(input));

                Config conf = new Config();
                conf.setNumWorkers(4);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(iLocalCluster, topology, completeTopologyParam);

                List<Values> valueses = Testing.readTuples(result, "boltB");

                assertNotNull(valueses);

                List v = valueses.get(0);
                assertNotNull(v);

                // check whether the result is right
                String expectedText = input
                                          + ", processed by com.baidu.oped.aqueducts.bolt.BoltA"
                                          + ", processed by com.baidu.oped.aqueducts.bolt.BoltB";
                assertEquals(expectedText, v.get(0).toString());
            }
        });
    }
}
