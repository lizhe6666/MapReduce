package combineFile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * @program: test
 * @description: mapper
 * @author: li zhe
 * @create: 2018-11-09 20:29
 */
public class combineMapper extends Mapper<NullWritable,BytesWritable,Text, BytesWritable> {
    private Text fileContent = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path path = fileSplit.getPath();
        fileContent.set(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(fileContent,value);
    }
}
