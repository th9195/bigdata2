package com.fiberhome.mapreduce.group;

import com.fiberhome.mapreduce.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<OrderBean, Text,Text, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //Text k3 = values.iterator().hasNext() ? values.iterator().next();
        Text k3 = null;
        int cout = 0 ;
        for (Text value : values) {
            k3 = value;
            cout++;
            if ( cout >= 1 ) {
                break;
            }
        }

        context.write(k3,NullWritable.get());
    }
}
