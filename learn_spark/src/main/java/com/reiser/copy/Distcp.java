package com.reiser.copy;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: reiserx
 * Date:2021/8/23
 * Des: Distcp 的 spark 实现
 */
public class Distcp {
    public static void main(String[] args) throws IOException, ParseException {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName(Distcp.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String s_path = args[args.length - 2];
        String t_path = args[args.length - 1];

        //获取配置参数
        int m = 3;
        boolean ignoreError = false;

        CommandLine cmdLine = getOptions(args);
        if (cmdLine.hasOption("m")) {
            m = Integer.parseInt(cmdLine.getOptionValue("m"));
        }

        if (cmdLine.hasOption("i")) {
            ignoreError = true;
        }

        Configuration configuration = sc.hadoopConfiguration();
        //还原输出路径
        FileSystem fs = FileSystem.get(configuration);

        Path srcPath = new Path(s_path);
        Path tarPath = new Path(t_path);
        if (fs.isDirectory(tarPath) && fs.listStatus(tarPath).length > 0 && !ignoreError) {
            System.err.println("the target dir not empty");
            System.exit(1);
        }

        //文件对应关系：files 中的 Tuple2 _1 是原文件路径 _2 是目标文件路径
        List<Tuple2<String, String>> files = new ArrayList<>();
        if (fs.isDirectory(srcPath)) {
            //检查文件夹是否存在
            checkDirectories(s_path, fs, s_path, t_path);
            //生成文件对应关系
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(srcPath, true);
            while (listFiles.hasNext()) {
                LocatedFileStatus next = listFiles.next();
                Path path = next.getPath();
                String fileSrcPathStr = next.getPath().toUri().getPath();
                String fileTarPathStr = t_path + fileSrcPathStr.split(s_path)[1];
                files.add(new Tuple2<>(path.toUri().getPath(), fileTarPathStr));
            }

        } else if (fs.isFile(srcPath)) {
            files.add(new Tuple2<>(s_path, t_path));
        } else {
            System.out.println(s_path + " is not a directory or file");
            System.exit(1);
        }

        if (!files.isEmpty()) {
            Broadcast<SerializableWritable<Configuration>> broadcast = sc.broadcast(new SerializableWritable<>(sc.hadoopConfiguration()));
            distCopy(sc, files, m, broadcast);
        }

        sc.close();

    }

    /**
     * 获取参数
     */
    private static CommandLine getOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("m", true, "max concurrence");
        options.addOption("i", false, "Ignore failures");

        CommandLineParser parser = new BasicParser();

        return parser.parse(options, args);
    }

    /**
     * 递归检查目标目录是否存在
     */
    private static void checkDirectories(String srcPathStr, FileSystem fs, String inputPathStr, String outPathStr) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(srcPathStr));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                String dirName = fileStatus.getPath().toString().split(inputPathStr)[1];
                Path newTarPath = new Path(outPathStr + dirName);
                if (!fs.exists(newTarPath)) {

                    fs.mkdirs(newTarPath);
                }
                checkDirectories(inputPathStr + dirName, fs, inputPathStr, outPathStr);
            }
        }
    }


    /**
     * 启动 spark 开始拷贝文件
     */
    private static void distCopy(JavaSparkContext sc, List<Tuple2<String, String>> files, int m, Broadcast<SerializableWritable<Configuration>> broadcast) {
        JavaRDD<String> copyResultRDD = sc.parallelize(files, m).mapPartitions(t -> {
            List<String> result = new ArrayList<>();
            Configuration configuration = broadcast.getValue().value();
            FileSystem fs = FileSystem.get(configuration);

            while (t.hasNext()) {
                Tuple2<String, String> next = t.next();
                Path src = new Path(next._1);
                Path dst = new Path(next._2);
                FileUtil.copy(fs, src, fs, dst, false, configuration);
                result.add("copy " + next._1 + " to " + next._2 + " success by worker #");
            }
            return result.iterator();
        });

        List<String> collect = copyResultRDD.collect();

        for (String s : collect) {
            System.out.println(s);
        }
    }
}
