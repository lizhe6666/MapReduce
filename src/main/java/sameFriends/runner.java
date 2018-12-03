package sameFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: test
 * @description: 运行
 * @author: li zhe
 * @create: 2018-11-09 22:47
 */
public class runner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(runner.class);

        job.setMapperClass(firstMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(firstReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job,new Path("E://Program Files//Spark//数据//inputFriends"));
        FileOutputFormat.setOutputPath(job,new Path("E://Program Files//Spark//数据//o"));

        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1);
    }
}
