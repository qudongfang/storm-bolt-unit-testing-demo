/*
 * Copyright (C) 2014 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.oped.aqueducts.bolt;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import junit.framework.TestCase;

/**
 * Author: qudongfang@baidu.com
 * Date: 2014:10:30 12:57:00
 */

public class BoltATest extends TestCase {

    public void testExecute() throws Exception {
        BoltA bolt = new BoltA();
        OutputCollector collector = mock(OutputCollector.class);
        Tuple tuple = mock(Tuple.class);
        String input = "boltA input";

        bolt.prepare(null, null, collector);

        when(tuple.getStringByField("value")).thenReturn(input);
        bolt.execute(tuple);

        // verifyNoMoreInteractions(collector);

        // verify(collector, atLeastOnce()).emit(any(Values.class));

        ArgumentCaptor<Values> argumentCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector, atLeastOnce()).emit(argumentCaptor.capture());
        // System.out.println(argumentCaptor.getAllValues().toString());
        String expected = "boltA input, processed by com.baidu.oped.aqueducts.bolt.BoltA";

        assertEquals(new Values(expected), argumentCaptor.getAllValues().get(0));
    }
}