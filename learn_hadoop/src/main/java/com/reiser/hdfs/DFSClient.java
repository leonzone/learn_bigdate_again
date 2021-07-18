package com.reiser.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author: reiserx
 * Date:2021/7/17
 * Des:
 */
public class DFSClient {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "stduent");

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), false);

        while (files.hasNext()){
            System.out.println(files.next());
        }

    }
}
