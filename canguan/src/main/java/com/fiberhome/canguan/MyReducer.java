package com.fiberhome.canguan;


import com.fiberhome.canguan.bean.MyCanGuan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;





public class MyReducer extends Reducer<MyCanGuan, NullWritable, Text,NullWritable> {
    @Override
    protected void reduce(MyCanGuan key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.toString()),NullWritable.get());
    }
}
