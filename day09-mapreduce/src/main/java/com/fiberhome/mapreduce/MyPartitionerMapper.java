package com.fiberhome.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyPartitionerMapper extends Mapper<LongWritable,Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1- 获取k2   v1 就是k2
        // 2- 获取v2   v2 就是nullWritable;
        // 3- 写入上下文中
        context.write(value,NullWritable.get());
    }
}
