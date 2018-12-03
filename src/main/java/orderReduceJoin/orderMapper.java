package orderReduceJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @program: test
 * @description: mapper
 * @author: li zhe
 * @create: 2018-11-08 20:37
 */
public class orderMapper extends Mapper<LongWritable,Text,IntWritable,Table> {
    private Table table = null;
    private IntWritable p_id = new IntWritable();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit =  (FileSplit) context.getInputSplit();
        String name = fileSplit.getPath().getName();
        String[] split = value.toString().split("\t");
        if(name.contains("order")){
            p_id.set(Integer.parseInt(split[1]));
            table = new Table();
            table.setId(Integer.parseInt(split[0]));
            table.setP_id(Integer.parseInt(split[1]));
            table.setCount(Integer.parseInt(split[2]));
            table.setP_name(" ");
            table.setFlag(1);
        }else{
            System.out.println(split[0] + split[1]+"--------------");
            table = new Table();
            p_id.set(Integer.parseInt(split[0]));
            table.setId(0);
            table.setP_id(Integer.parseInt(split[0]));
            table.setCount(0);
            table.setP_name(split[1]);
            table.setFlag(0);
            System.out.println(table);
        }
        context.write(p_id,table);
    }
}
