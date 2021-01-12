package com.fiberhome.mapreduce.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class MapreduceUtils {

    /**
     * 删除已经存在的数据路径
     * @param outputPath 输出路径
     */
    public static void deleteOutputPath(Path outputPath) {

        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            boolean exists = fileSystem.exists(outputPath);
            if (exists ) {
                fileSystem.delete(outputPath,true);
            }
        }catch ( Exception e){
            e.printStackTrace();
        }
    }
}
