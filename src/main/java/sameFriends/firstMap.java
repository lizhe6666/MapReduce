package sameFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: test
 * @description: 第一次 map
 * @author: li zhe
 * @create: 2018-11-09 22:24
 */
public class firstMap extends Mapper<LongWritable,Text,Text,Text> {
    private Text user = new Text();
    private Text friend = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(":");
        user.set(line[0]);
        String[] friends = line[1].split(",");
        for (String str:friends) {
            friend.set(str);
            context.write(friend,user);
        }
    }
}
