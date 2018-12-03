package orderReduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: test
 * @description: 表单
 * @author: li zhe
 * @create: 2018-11-08 20:16
 */
public class Table implements Writable {

    private int id;
    private int p_id;
    private String p_name;
    private int count;
    private int flag;
    public Table() {
        super();
    }

    public Table(int id, int p_id, String p_name, int count, int glag) {
        this.id = id;
        this.p_id = p_id;
        this.p_name = p_name;
        this.count = count;
        this.flag = glag;
    }

    @Override
    public String toString() {
        return id + "\t" + p_name + "\t" + count + "\t";
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(p_name);
        dataOutput.writeInt(p_id);
        dataOutput.writeInt(count);
        dataOutput.writeInt(flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.p_name = dataInput.readUTF();
        this.p_id = dataInput.readInt();
        this.count = dataInput.readInt();
        this.flag = dataInput.readInt();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getP_id() {
        return p_id;
    }

    public void setP_id(int p_id) {
        this.p_id = p_id;
    }

    public String getP_name() {
        return p_name;
    }

    public void setP_name(String p_name) {
        this.p_name = p_name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
