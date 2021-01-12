package com.fiberhome.mapreduce;

import com.fiberhome.mapreduce.bean.Employe;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * k2--- Employe
 * v2--- NullWritable
 *
 *
 * k3--- Text
 * v3--- NullWritable
 */
public class EmployeReducer extends Reducer<Employe, NullWritable, Text,NullWritable> {
    @Override
    protected void reduce(Employe key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        // 获取k3 v3 并写入context;  k2 就是k3
        context.write(new Text(key.toString()),NullWritable.get());

    }
}
