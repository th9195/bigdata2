package com.fiberhome.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;


public class JobMain {

    public static void main(String[] args) throws Exception {
        // 1- 获取job任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"partitioner");

        job.setJarByClass(JobMain.class);

        job.setInputFormatClass(TextInputFormat.class);

        // windows
//        TextInputFormat.setInputPaths(job,new Path("file:///E:\\input\\partitioner"));
        TextInputFormat.setInputPaths(job,new Path("hdfs://node1:8020/input/partitioner"));


        job.setMapperClass(MyPartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);


         // 5- 指定分区类
        job.setPartitionerClass(MyPartitioner.class);

        job.setReducerClass(MyPartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reduce个数
        job.setNumReduceTasks(2);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path("hdfs://node1:8020/output/partitioner");
        deleteOutputPath(outputPath);
        TextOutputFormat.setOutputPath(job,outputPath);
        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }

    private static void deleteOutputPath(Path outputPath) {
        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            boolean exists = fileSystem.exists(outputPath);
            System.out.println("***********exists == " + exists);
            if ( exists ) {
                fileSystem.delete(outputPath,true);
            }

        }catch ( Exception e){
            e.printStackTrace();

        }

    }
}
