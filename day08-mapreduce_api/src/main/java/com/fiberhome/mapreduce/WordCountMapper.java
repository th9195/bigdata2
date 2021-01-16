package com.fiberhome.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


// 写一个类去继承mapper

/**
 * Mapper 四个泛型
 * K1  :LongWritable    行偏移量
 * V1  :Text            行文本数据
 * K2  :Text            每个单词
 * V2  :LongWritable    固定值1
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    public static long countMapper = 0;

    // 2- 重写map方法， 将K1 V1 转成 K2 V2

    /**
     *
     * @param k1
     * @param v1
     * @param context  上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        // 1- 得到k2   ---->  按照逗号切分V1,得到的每个单词就是K2;
        String[] words = v1.toString().split(",");
        // 2- 得到V2    ---> V2 就是固定值1

        // 3- 将k2 v2 写入上下文中
        Text k2 = new Text();
        LongWritable v2 = new LongWritable(1);
        for (String word : words) {
            k2.set(word);
            context.write(k2,v2);
        }


        countMapper++ ;
        System.out.println("countMapper == " + countMapper);


    }
}
