package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: test
 * @description: map端 聚合
 * @author: li zhe
 * @create: 2018-11-16 21:01
 */
public class combiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable it: values) {
            System.out.println("xiaoxiao");
            sum += it.get();
        }
        context.write(key,new IntWritable(sum));

        String filter_percentage = context.getConfiguration().get("filter_percentage");
    }
}
