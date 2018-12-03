package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: test
 * @description: map 端
 * @author: li zhe
 * @create: 2018-11-08 19:35
 */
public class wcMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split(" ");
        for (String fiked:fileds) {
            context.write(new Text(fiked),new IntWritable(1));
        }
    }

}
