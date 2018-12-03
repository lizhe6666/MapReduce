package orderReduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: test
 * @description: reducer
 * @author: li zhe
 * @create: 2018-11-08 20:55
 */
public class orderReducer extends Reducer<IntWritable,Table,Text,NullWritable> {
    private Text text = new Text();
    @Override
    protected void reduce(IntWritable key, Iterable<Table> values, Context context) throws IOException, InterruptedException {
        List<Table> tables = new ArrayList<Table>();
        Table p_bean = null;
        for (Table table :values) {
            if(table.getFlag() == 1){
//                Table orderBean = new Table();
//                try {
//                    BeanUtils.copyProperties(orderBean,table);
//                } catch (IllegalAccessException e) {
//                    e.printStackTrace();
//                } catch (InvocationTargetException e) {
//                    e.printStackTrace();
//                }
                Table orderBean = new Table(table.getId(),table.getP_id(),table.getP_name(),
                        table.getCount(),table.getFlag());
                tables.add(orderBean);
                System.out.println(table + "**********");
            }else {
//                try {
//                    BeanUtils.copyProperties(p_bean,table);
//                } catch (IllegalAccessException e) {
//                    e.printStackTrace();
//                } catch (InvocationTargetException e) {
//                    e.printStackTrace();
//                }
                p_bean = new Table(table.getId(),table.getP_id(),table.getP_name(),
                        table.getCount(),table.getFlag());
                System.out.println(p_bean.getP_name()+"--------");
            }
        }

        for (Table table : tables) {
            System.out.println(table.getId()+"\t"+p_bean.getP_name()+"\t"+table.getCount()+"\t"+"++++");
            text.set(table.getId()+"\t"+p_bean.getP_name()+"\t"+table.getCount()+"\t");
            context.write(text,NullWritable.get());
        }
    }
}
