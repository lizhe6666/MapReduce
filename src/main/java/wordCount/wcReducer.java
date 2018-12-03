package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: test
 * @description: reducer 端
 * @author: li zhe
 * @create: 2018-11-08 19:44
 */
public class wcReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable it: values) {
            System.out.println("1111");
            sum += it.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
