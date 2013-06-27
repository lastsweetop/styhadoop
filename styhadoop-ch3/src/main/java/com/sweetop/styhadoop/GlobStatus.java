package com.sweetop.styhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-6-3
 * Time: 下午2:37
 * To change this template use File | Settings | File Templates.
 */
public class GlobStatus {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        FileStatus[] status = fs.globStatus(new Path(uri),new RegexExludePathFilter("^.*/1901"));
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }

    }
}
