package com.sweetop.styhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-6-25
 * Time: 下午10:09
 * To change this template use File | Settings | File Templates.
 */
public class StreamCompressor {
    public static void main(String[] args) throws Exception {
        String codecClassName = args[0];
        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        CompressionOutputStream out = codec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in, out, 4096, false);

        out.finish();

    }
}
