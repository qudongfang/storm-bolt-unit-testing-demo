/*
 * Copyright (C) 2014 Baidu, Inc. All Rights Reserved.
 */

package com.baidu.oped.aqueducts.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;

import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * rewrite based on io.dataplay.test.TupleUtil
 * <p/>
 * Author: qudongfang@baidu.com
 * Date: 2014:10:13 13:18:00
 */

public final class MockTupleHelpers {

    /**
     * Private constructor.
     */
    private MockTupleHelpers() {
    }

    /**
     * Creates a mock tick tuple.
     *
     * @return A new tick tuple.
     */
    public static Tuple mockTickTuple() {
        return mockTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * Create a mock tuple for a given component and stream.
     *
     * @param componentId The Component ID.
     * @param streamId    The Stream ID
     *
     * @return A new tuple.
     */
    public static Tuple mockTuple(final String componentId, final String streamId) {
        return mockTuple(componentId, streamId, new Fields(), new ArrayList<>());
    }

    /**
     * Create a mock tuple for a given component, stream, with data and fields.
     *
     * @param componentId The Component ID.
     * @param streamId    The Stream ID.
     * @param fields      Fields.
     * @param data        The data to include in the tuple.
     *
     * @return A new tuple.
     */
    public static Tuple mockTuple(final String componentId, final String streamId, final Fields fields,
                                  final List<Object> data) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(componentId);
        when(tuple.getSourceStreamId()).thenReturn(streamId);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValues()).thenReturn(data);

        int fieldCount = fields.size();
        for (int i = 0; i < fieldCount; i++) {
            when(tuple.getValue(i)).thenReturn(data.get(i));
            when(tuple.getString(i)).thenReturn(data.get(i).toString());
        }

        for (String field : fields) {
            int idx = fields.fieldIndex(field);
            when(tuple.getValueByField(field)).thenReturn(data.get(idx));
        }

        return tuple;
    }

    /**
     * Generate a generic tuple with random data.
     *
     * @return A mock data tuple with random data.
     */
    public static Tuple mockDataTuple() {

        List<String> fieldData = new ArrayList<>();
        List<Object> valueData = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            fieldData.add(RandomStringUtils.randomAlphanumeric(5));
            valueData.add(RandomStringUtils.randomAlphanumeric(10));
        }

        return mockTuple(Constants.SYSTEM_EXECUTOR_ID.toString(), Utils.DEFAULT_STREAM_ID, new Fields(fieldData),
                            valueData);
    }

    /**
     * Generate a generic tuple with specific data.
     *
     * @param fields Fields.
     * @param data   The data to include in the tuple.
     *
     * @return A mock data tuple with random data.
     */
    public static Tuple mockDataTuple(final List<String> fields, final List<Object> data) {
        return mockTuple(Constants.SYSTEM_EXECUTOR_ID.toString(), Utils.DEFAULT_STREAM_ID, new Fields(fields), data);
    }

    /**
     * Generate a generic tuple with specific data.
     *
     * @param fields Fields.
     * @param data   The data to include in the tuple.
     *
     * @return A mock data tuple with random data.
     */
    public static Tuple mockDataTuple(final String[] fields, final Object[] data) {

        List<String> fieldList = Arrays.asList(fields);
        List<Object> dataList = Arrays.asList(data);

        return mockTuple(Constants.SYSTEM_EXECUTOR_ID.toString(), Utils.DEFAULT_STREAM_ID, new Fields(fieldList),
                            dataList);
    }
}