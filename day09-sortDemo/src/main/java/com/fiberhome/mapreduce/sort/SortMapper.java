package com.fiberhome.mapreduce.sort;

import com.fiberhome.mapreduce.bean.SortBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arrs = value.toString().split("\t");
        SortBean sortBean = new SortBean();
        sortBean.setName(arrs[0]);
        sortBean.setAge(Integer.parseInt(arrs[1]));

        context.write(sortBean,NullWritable.get());
    }
}
