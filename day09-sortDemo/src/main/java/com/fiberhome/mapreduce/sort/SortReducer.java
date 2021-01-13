package com.fiberhome.mapreduce.sort;

import com.fiberhome.mapreduce.bean.SortBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortBean, NullWritable, Text,NullWritable> {

    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(new Text(key.toString()),NullWritable.get());

    }
}
