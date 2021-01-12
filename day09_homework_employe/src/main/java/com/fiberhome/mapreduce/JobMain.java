package com.fiberhome.mapreduce;

import com.fiberhome.mapreduce.Utils.MapreduceUtils;
import com.fiberhome.mapreduce.bean.Employe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.soap.Text;


public class JobMain {

    public static void main(String[] args) throws Exception {

        // 1- 获取一个job任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "employe");

        // 2- 指定job任务的jar包
        job.setJarByClass(JobMain.class);

        // 3- 指定源文件读取文件方式类 和 路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node1:8020/input/employe"));

        // 4- 指定Mapper类型 和 K2 K2类型
        job.setMapperClass(EmployeMapper.class);
        job.setMapOutputKeyClass(Employe.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5- 指定分区类
        job.setPartitionerClass(EmployePartitioner.class);

        // 6- 指定Reducer类和 k3 v3类型
        job.setReducerClass(EmployeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 7- 指定Reduce个数
        job.setNumReduceTasks(4);

        // 8- 指定文件输出方式类 和 路径
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path("hdfs://node1:8020/output/employe");
        // 8-1 如果输入路径已经存在 就先删除输出路径
        MapreduceUtils.deleteOutputPath(outputPath);
        // 8-2 设置输出路径
        TextOutputFormat.setOutputPath(job,outputPath);

        // 9- 将job提交到yarn集群
        boolean b = job.waitForCompletion(true);

        // 10- 退出执行进程
        System.exit(b?0:1);

    }
}
