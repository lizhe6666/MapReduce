package sameFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: test
 * @description: 第二次 Map
 * @author: li zhe
 * @create: 2018-11-10 09:07
 */
public class secondMap extends Mapper<LongWritable,Text,Text,Text> {
    private Text user = new Text();
    private Text realations = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(":");
        user.set(line[0]);
        String[] fiends = line[1].split(",");
        for (int i = 0; i < fiends.length - 1; i++)
            for (int j = i + 1; j < fiends.length; j++){
                if(fiends[i].charAt(0) - fiends[j].charAt(0) > 0){
                    realations.set(fiends[j]+"-"+fiends[i]+":");
                }else {
                    realations.set(fiends[i]+"-"+fiends[j]+":");
                }
                context.write(realations,user);
        }
    }
}
