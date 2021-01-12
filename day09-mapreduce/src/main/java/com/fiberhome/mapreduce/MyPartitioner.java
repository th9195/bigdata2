package com.fiberhome.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区： 就是决定每一个k2 v2到最后去到哪个reduce,也就是一个分区
 */

// 1- 自定义类继承Partitioner类
public class MyPartitioner extends Partitioner<Text, NullWritable> {

    // 2- 重写getPartition方法，指定分区规则

    /**
     *
     * @param text              k2
     * @param nullWritable      v2
     * @param i                 reduce个数
     * @return                  就是指定参数传进来的k2 v2应该去到哪个reduce,也就是哪个分区
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {

        // 1- 拆分k2
        String[] strArr = text.toString().split("\t");
        // 2- 判断index = 5 的数据是否 大于 15
        if ( Integer.parseInt(strArr[0]) < 15 ) {
            return 0;
        }else {
            return 1;
        }

    }
}
