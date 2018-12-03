package orderReduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @program: test
 * @description: driver
 * @author: li zhe
 * @create: 2018-11-08 21:05
 */
public class runner {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(runner.class);

        job.setMapperClass(orderMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Table.class);

        job.setReducerClass(orderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job,new Path("E://Program Files//Spark//数据//input1"));
        FileOutputFormat.setOutputPath(job,new Path("E://Program Files//Spark//数据//outPut"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
