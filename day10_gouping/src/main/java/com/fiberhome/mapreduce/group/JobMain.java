package com.fiberhome.mapreduce.group;

import com.fiberhome.mapreduce.Utils.MapreduceUtils;
import com.fiberhome.mapreduce.bean.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

public class JobMain {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "orderGrouping");

        job.setJarByClass(JobMain.class);

        // 输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node1:8020/input/orderList"));


        // mapper
        job.setMapperClass(GroupingMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);


        // partitioner

        // combiner

        // grouping
        job.setGroupingComparatorClass(MyGrouping.class);

        // reducer
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        // 输出
        Path outputPath = new Path("hdfs://node1:8020/output/orderList");
        MapreduceUtils.deleteOutputPath(outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,outputPath);


        // 开始执行
        boolean b = job.waitForCompletion(true);

        // 退出
        System.exit(b?0:1);


    }



}
