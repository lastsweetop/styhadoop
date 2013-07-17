package com.sweetop.styhadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.eclipse.jdt.internal.core.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-5
 * Time: 上午1:26
 * To change this template use File | Settings | File Templates.
 */
public class TestComparator {
    RawComparator<IntWritable> comparator;
    IntWritable w1;
    IntWritable w2;

    /**
     * 获得IntWritable的comparator,并初始化两个IntWritable
     */
    @Before
    public void init() {
        comparator = WritableComparator.get(IntWritable.class);
        w1 = new IntWritable(163);
        w2 = new IntWritable(76);
    }

    /**
     * 比较两个对象大小
     */
    @Test
    public void testComparator() {
        Assert.isTrue(comparator.compare(w1, w2) > 0);
    }

    /**
     * 序列化后进行直接比较
     *
     * @throws IOException
     */
    @Test
    public void testcompare() throws IOException {
        byte[] b1 = SerializeUtils.serialize(w1);
        byte[] b2 = SerializeUtils.serialize(w2);
        Assert.isTrue(comparator.compare(b1, 0, b1.length, b2, 0, b2.length) > 0);
    }

}
