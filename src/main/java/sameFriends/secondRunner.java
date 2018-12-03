package sameFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: test
 * @description: 第二次运行
 * @author: li zhe
 * @create: 2018-11-10 19:20
 */
public class secondRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(secondRunner.class);

        job.setMapperClass(secondMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(secondReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job,new Path("E://Program Files//Spark//数据//inputSecond"));
        FileOutputFormat.setOutputPath(job,new Path("E://Program Files//Spark//数据//inputSecond//xiaoxiao"));

        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1);

    }
}
