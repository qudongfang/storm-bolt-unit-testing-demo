/*
 * Copyright (C) 2014 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.oped.aqueducts.bolt;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.mockito.ArgumentCaptor;

import backtype.storm.tuple.Tuple;
import junit.framework.TestCase;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * Author: qudongfang@baidu.com
 * Date: 2014:10:30 14:45:00
 */

public class BoltCTest extends TestCase {

    public void testExecute() throws Exception {
        BoltC bolt = new BoltC();
        Tuple tuple = mock(Tuple.class);
        Producer<String, String> PRODUCER = mock(Producer.class);
        String input = "boltC input";

        // Field[] fields = Producer.class.getFields();

        Field privateField = BoltC.class.getDeclaredField("PRODUCER");
        privateField.setAccessible(true);
        privateField.set(bolt, PRODUCER);

        when(tuple.getStringByField("value")).thenReturn(input);
        bolt.execute(tuple, null);

        // check output method 1
        verify(PRODUCER, atLeastOnce()).send(any(KeyedMessage.class));

        // check output method 2
        ArgumentCaptor<KeyedMessage> argumentCaptor = ArgumentCaptor.forClass(KeyedMessage.class);
        verify(PRODUCER, atLeastOnce()).send(argumentCaptor.capture());

        // assertEquals(input, argumentCaptor.getAllValues().get(0));
    }
}