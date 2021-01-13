package com.fiberhome.mapreduce.group;

import com.fiberhome.mapreduce.bean.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupingMapper extends Mapper<LongWritable, Text, OrderBean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1- 获取k2
        OrderBean orderBean = new OrderBean();
        String[] arrs = value.toString().split(",");
        orderBean.setOrderId(arrs[0]);
        orderBean.setMoney(Double.parseDouble(arrs[2]));

        // 2- 获取v2
        Text v2 = value;

        // 3- 写入context;
        context.write(orderBean,v2);

    }
}
