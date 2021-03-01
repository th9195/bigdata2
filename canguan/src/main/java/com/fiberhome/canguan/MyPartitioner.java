package com.fiberhome.canguan;

import com.fiberhome.canguan.bean.MyCanGuan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyCanGuan, NullWritable> {


    @Override
    public int getPartition(MyCanGuan myCanGuan, NullWritable nullWritable, int i) {

        int result = 0;
        String year = myCanGuan.getYear();
        switch (year){
            case "2019年":
                result = 0 ;
                break;
            case "2020年":
                result = 1 ;
                break;
            default:
                System.out.println("error data");
                result = 3 ;
                break;

        }


        return result;
    }
}
