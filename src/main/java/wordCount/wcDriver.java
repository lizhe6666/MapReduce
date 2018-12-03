package wordCount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @program: test
 * @description: driver
 * @author: li zhe
 * @create: 2018-11-08 19:50
 */
public class wcDriver {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(wcDriver.class);

        job.setMapperClass(wcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(combiner.class);

        job.setReducerClass(wcReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job,new Path("E://Program Files//Spark//数据//input"));
        FileOutputFormat.setOutputPath(job,new Path("E://Program Files//Spark//数据//l"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
