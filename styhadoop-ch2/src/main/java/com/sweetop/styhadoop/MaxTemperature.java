package com.sweetop.styhadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-5-22
 * Time: 下午8:47
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperature {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length!=2){
            System.out.println("Usage: MaxTemperature <input path> <out path>");
            System.exit(-1);
        }
        Job job=new Job();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max Temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatrueMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
