package com.reiser.mapr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @author: reiserx
 * Date:2021/7/14
 * Des:
 */
public class Flow {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //初始化 job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Flow");
        //设置 mapper 和 reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //指定输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path path = new Path(args[1]);

        try {
            FileUtil.fullyDelete(new File(args[1]));
        } catch (Exception e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job, path);


        //指定 MapOutput kv 类型 - 必须
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setJarByClass(Flow.class);

        //启动任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //把一行数据根据空格切分成多个单词，并以 <word,1> 的形式输出
            String line = value.toString();
            String[] words = line.split("\t");
            FlowBean flowBean = new FlowBean();
            k.set(words[1]);
            flowBean.setUpFlow(Long.parseLong(words[8]));
            flowBean.setDownFlow(Long.parseLong(words[9]));
            flowBean.setSumFlow(Long.parseLong(words[8]) + Long.parseLong(words[9]));
            context.write(k, flowBean);
        }
    }

    public static class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            int up = 0;
            int down = 0;
            int sum = 0;

            for (FlowBean value : values) {
                up += value.getUpFlow();
                down += value.getDownFlow();
                sum += value.getSumFlow();
            }
            Text out = new Text(up + " " + down + " " + sum);
            context.write(key, out);
        }
    }
}
