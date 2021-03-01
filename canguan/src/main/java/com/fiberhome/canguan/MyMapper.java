package com.fiberhome.canguan;

import com.fiberhome.canguan.bean.MyCanGuan;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, MyCanGuan, NullWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueStr = value.toString();
        String[] valueArr = valueStr.split(",");



        // 需求一： 去除源文件中缺失的数据；
        for (String s : valueArr) {
            if (null == s || "".equals(s) ) {
                return;
            }
        }


        MyCanGuan myCanGuan = new MyCanGuan();
        myCanGuan.setName(valueArr[0]);
        myCanGuan.setIncome(Integer.parseInt(valueArr[1]));
        myCanGuan.setExpenditure(Integer.parseInt(valueArr[2]));
        myCanGuan.setYear(valueArr[3]);

        // 需求四： 后面加两个字段 净营业额 标记
        myCanGuan.setInOrout(myCanGuan.getIncome()-myCanGuan.getExpenditure());
        myCanGuan.setInOroutStr(myCanGuan.getInOrout()>0?"盈利":"亏损");
        context.write(myCanGuan,NullWritable.get());

    }
}
