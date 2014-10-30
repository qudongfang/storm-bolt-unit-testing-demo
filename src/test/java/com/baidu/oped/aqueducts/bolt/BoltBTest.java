/*
 * Copyright (C) 2014 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.oped.aqueducts.bolt;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import junit.framework.TestCase;

/**
 * Author: qudongfang@baidu.com
 * Date: 2014:10:30 13:20:00
 */

public class BoltBTest extends TestCase {

    public void testExecute() throws Exception {
        BoltB bolt = new BoltB();
        BasicOutputCollector collector = mock(BasicOutputCollector.class);
        Tuple tuple = mock(Tuple.class);
        String input = "boltB input";

        // case 1
        when(tuple.getStringByField("value")).thenReturn(null);
        bolt.execute(tuple, collector);
        verifyZeroInteractions(collector);
        // verifyNoMoreInteractions(collector);

        // case 2
        when(tuple.getStringByField("value")).thenReturn(input);
        bolt.execute(tuple, collector);

        // check output method 1
        // verify(collector, atLeastOnce()).emit(any(Values.class));

        // check output method 2
        ArgumentCaptor<Values> argumentCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector, atLeastOnce()).emit(argumentCaptor.capture());
        // System.out.println(argumentCaptor.getAllValues().toString());
        String expected = "boltB input, processed by com.baidu.oped.aqueducts.bolt.BoltB";

        assertEquals(new Values(expected), argumentCaptor.getAllValues().get(0));
    }
}