package com.fiberhome.mapreduce;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *  1- 自己定义类继承Reduer类
 *  2- 重写reduce方法，在方法中将 k2 [v2] 转成 k2 v3
 *
 *  Reducer四个泛型
 *  k2  :Text               单词
 *  v2  :LongWritable       固定值1
 *  k3  :Text               单词
 *  v3  :LongWritable       单词出现次数
 */
public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

<<<<<<< HEAD
    public static long countReducer = 0;
=======

>>>>>>> 914c2b7e90177aded7d833dfc6cfcd8ae809b5c0
    // 2-  重写reduce方法，在方法中将 k2 [v2] 转成 k2 v3
    /**
     *
     * @param k2       k2
     * @param v2Arr    [v2]
     * @param context   上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text k2, Iterable<LongWritable> v2Arr, Context context) throws IOException, InterruptedException {

        // 1- 得到k3  ---> k3 == k2

        // 2- 得到v3 v2的长度就是V3
        long v = 0;
        for (LongWritable v2 : v2Arr) {
            v += v2.get();
        }
        LongWritable v3 = new LongWritable(v);
        // 3- 将k3 v3 写入上下文中
        context.write(k2,v3);
<<<<<<< HEAD


        countReducer++;
        System.out.println("countReducer == " + countReducer);
=======
>>>>>>> 914c2b7e90177aded7d833dfc6cfcd8ae809b5c0
    }
}
