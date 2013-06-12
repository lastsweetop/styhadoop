package com.sweetop.styhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-6-2
 * Time: 下午8:58
 * To change this template use File | Settings | File Templates.
 */
public class ShowFileStatus {

    public static void main(String[] args) throws IOException {
        Path path = new Path(args[0]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        FileStatus status = fs.getFileStatus(path);
        System.out.println("path = " + status.getPath());
        System.out.println("owner = " + status.getOwner());
        System.out.println("block size = " + status.getBlockSize());
        System.out.println("permission = " + status.getPermission());
        System.out.println("replication = " + status.getReplication());
    }
}
