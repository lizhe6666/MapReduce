package combineFile;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @program: test
 * @description: 运行类
 * @author: li zhe
 * @create: 2018-11-09 20:37
 */
public class combineRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(combineRunner.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(combineMapper.class);

        FileInputFormat.addInputPath(job,new Path("E://Program Files//Spark//数据//inputCombine"));
        FileOutputFormat.setOutputPath(job,new Path("E://Program Files//Spark//数据//out"));

        boolean result = job.waitForCompletion(true);
        System.out.println(result? 0:1);

    }
}
