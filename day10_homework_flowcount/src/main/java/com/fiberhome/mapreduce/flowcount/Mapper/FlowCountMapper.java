package com.fiberhome.mapreduce.flowcount.Mapper;

import com.fiberhome.mapreduce.flowcount.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("*****************start mapper******************");
        String[] arrs = value.toString().split("\t");

        Text k2 = new Text(arrs[1]);

        FlowBean flowBean = new FlowBean();
        flowBean.setUpPackageNum(Long.parseLong(arrs[6]));
        flowBean.setDownPackageNum(Long.parseLong(arrs[7]));
        flowBean.setUpPayLoad(Long.parseLong(arrs[8]));
        flowBean.setDownPayLoad(Long.parseLong(arrs[9]));


        System.out.println(flowBean);
        context.write(k2,flowBean);

        System.out.println("******************end mapper*****************");
    }
}
