package orderMapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * @program: test
 * @description: map join
 * @author: li zhe
 * @create: 2018-11-08 22:32
 */
public class orderMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    private Text content = new Text();
    private HashMap<String,String> hashMap = new HashMap<String, String>();
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        System.out.println(localCacheFiles[0].toString()+"********");
        BufferedReader bufferedReader = new BufferedReader
                (new InputStreamReader(new FileInputStream(localCacheFiles[0].toString())));
        String line;
        while(StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            // 2 切割
            String[] fields = line.split("\t");

            // 3 缓存数据到集合
            hashMap.put(fields[0], fields[1]);
        }

        // 4 关流
        bufferedReader.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        content.set(split[0]+"\t"+hashMap.get(split[1])+"\t"+split[2]);
        context.write(content,NullWritable.get());
    }
}
