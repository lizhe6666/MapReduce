package orderMapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInputStream;
import java.net.URI;


/**
 * @program: test
 * @description: 运行
 * @author: li zhe
 * @create: 2018-11-08 22:40
 */
public class disRunner {
    public static void main(String[] args) throws Exception{
        Configuration conf  = new Configuration();
        DistributedCache.addCacheFile(new Path("file:///E:/pd.txt").toUri(),conf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(disRunner.class);

        job.setMapperClass(orderMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("E://Program Files//Spark//数据//input1//order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E://Program Files//Spark//数据//output9"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
