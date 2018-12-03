package sameFriends;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: test
 * @description: 第一次 reduce
 * @author: li zhe
 * @create: 2018-11-09 22:32
 */
public class firstReduce extends Reducer<Text,Text,Text,NullWritable> {
    private Text content = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer(key.toString()+":");
        for (Text text:values) {
            stringBuffer.append(text.toString()+",");
        }
        content.set(stringBuffer.substring(0,stringBuffer.length() - 1));
        context.write(content,NullWritable.get());
    }
}
