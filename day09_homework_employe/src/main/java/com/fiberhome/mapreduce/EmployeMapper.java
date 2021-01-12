package com.fiberhome.mapreduce;

import com.fiberhome.mapreduce.Utils.DateFormatUtils;
import com.fiberhome.mapreduce.bean.Employe;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * k1--- LongWritable
 * v1--- Text
 *
 * k2--- Employe
 * v2--- NullWritable
 */
public class EmployeMapper extends Mapper<LongWritable, Text, Employe,NullWritable> {

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        // 1- 从k1 中切分并封装bean
        Employe employe = getBean(v1.toString());
        if ( null != employe ) {
            context.write(employe, NullWritable.get());
        }
    }

    /**
     * 将字符串中的每个元素封装成一个Bean
     * @param beanStr
     * @return
     */
    private Employe getBean(String beanStr) {
        Employe employe = new Employe();
        System.out.println(beanStr);
        String[] eleArr = beanStr.split(",");

        // 需求1: 剔除源文件中员工编号为空的员工信息
        if ( "null".equalsIgnoreCase(eleArr[0]) ) {
            return  null;
        }

        employe.setEmployeId(eleArr[0]);
        employe.setName(eleArr[1]);
        employe.setStatment(eleArr[2]);
        employe.setSuperiorId(eleArr[3]);
        employe.setEntryDay(DateFormatUtils.strToDate("dd-MM月-yy",eleArr[4]));

        // 判断薪水和提成是不是数字，如果不是数字 丢弃这条数据
        boolean isnum = NumberUtils.isNumber(eleArr[5]) && NumberUtils.isNumber(eleArr[6]);
        if ( !isnum ) {
            return null;
        }
        employe.setSalary(Double.parseDouble(eleArr[5]));
        employe.setExtraSalary(Double.parseDouble(eleArr[6]));
        employe.setStatmentId(eleArr[7]);

        return  employe;
    }   
}
