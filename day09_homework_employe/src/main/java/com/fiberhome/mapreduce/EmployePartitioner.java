package com.fiberhome.mapreduce;

import com.fiberhome.mapreduce.bean.Employe;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class EmployePartitioner extends Partitioner<Employe, NullWritable> {
    @Override
    public int getPartition(Employe employe, NullWritable nullWritable, int i) {


        // 需求2：根据部分编号将源文件分类管理，同一个部门的员工信息放在一起
        String statmentId = employe.getStatmentId();
        int result = 0;  // 默认没有部门为0
        switch (statmentId){
            case "10":
                result = 1;
                break;

            case "20":
                result = 2;
                break;

            case "30":
                result = 3;
                break;
            default:
                System.out.println("没有该部门.");
        }

        return result;
    }
}
