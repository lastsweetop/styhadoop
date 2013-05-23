package com.sweetop.styhadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-5-22
 * Time: 下午8:39
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxvalue = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxvalue = Math.max(maxvalue, value.get());
        }
        context.write(key, new IntWritable(maxvalue));
    }
}
