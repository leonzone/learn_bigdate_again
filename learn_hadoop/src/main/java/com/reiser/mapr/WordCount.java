package com.reiser.mapr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: reiserx
 * Date:2021/7/14
 * Des:
 */
public class WordCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //初始化 job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        //设置 mapper 和 reducer
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        //指定输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //指定 MapOutput kv 类型 - 必须
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setJarByClass(WordCount.class);

        //启动任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //把一行数据根据空格切分成多个单词，并以 <word,1> 的形式输出
            String line = value.toString();
            String[] words = line.split(" ");

            for (String word : words) {
                k.set(word);
                context.write(k, v);
            }


        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable v = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //key 是 word,它们被汇总在了一起； values 的是汇总之后的值，数据形式是[1,1,1,1]
            int sum = 0;
            for (IntWritable value : values) {
                sum++;
            }

            v.set(sum);
            context.write(key, v);

        }
    }
}
