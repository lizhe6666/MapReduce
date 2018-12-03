package sameFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: test
 * @description: 第二次 reduce
 * @author: li zhe
 * @create: 2018-11-10 09:31
 */
public class secondReduce extends Reducer<Text,Text,Text,Text> {
    private Text relations = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer str = new StringBuffer();
        for (Text value:values) {
            str.append(value.toString()+",");
        }
        relations.set(str.substring(0,str.length() - 1));
        context.write(key,relations);
    }
}
