package com.fiberhome.mapreduce.flowcount.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class MapreduceUtils {

    /**
     * 判断改路径是否存在，如果存就删除
     * @param path
     */
    public static void deletePathIfExists(Path path){
        try{
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            boolean exists = fileSystem.exists(path);
            if (exists ) {
                fileSystem.delete(path,true);
            }
        }catch ( Exception e){
            e.printStackTrace();
        }

    }
}
