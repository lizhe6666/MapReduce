package combineFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @program: test
 * @description: 重写 recordReader
 * @author: li zhe
 * @create: 2018-11-09 20:05
 */
public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;
    private Configuration configuration;
    private BytesWritable value = new BytesWritable();
    private boolean processed = false;
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException {
        if(!processed){
            byte[] contents = new byte[(int) fileSplit.getLength()];

            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);

            FSDataInputStream fis = fileSystem.open(path);
            IOUtils.readFully(fis, contents, 0, contents.length);
            value.set(contents, 0, contents.length);
            IOUtils.closeStream(fis);
            processed = true;
            return  true;
        }
        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return processed?1:0;
    }

    public void close() throws IOException {

    }
}
