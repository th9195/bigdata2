package com.fiberhome.mapreduce.flowcount.main;

import com.fiberhome.mapreduce.flowcount.Mapper.FlowCountMapper;
import com.fiberhome.mapreduce.flowcount.Utils.MapreduceUtils;
import com.fiberhome.mapreduce.flowcount.bean.FlowBean;
import com.fiberhome.mapreduce.flowcount.reducer.FlowCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JobMain {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "flowcount");
        job.setJarByClass(JobMain.class);


        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node1:8020/input/flowcount"));

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path("hdfs://node1:8020/output/flowcount");
        MapreduceUtils.deletePathIfExists(outputPath);
        TextOutputFormat.setOutputPath(job,outputPath);

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);
    }

}
