package serializeDemo;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable.Comparator;

import java.io.*;

public class P00120_AccountWritable_0010 {
    public static void main(String[] args) {
        AccountWritable aw1 = new AccountWritable();
        aw1.set(new IntWritable(30), new Text("zyh"), new BooleanWritable(true));

        AccountWritable aw2 = new AccountWritable();
        aw2.set(new IntWritable(30), new Text("zyh"), new BooleanWritable(true));

        AccountWritable.DiyComparator comparator = new AccountWritable.DiyComparator();
        System.out.println(comparator.compare(aw1, aw2));
    }
}

class AccountWritable
        implements WritableComparable<AccountWritable> {

    private IntWritable code;
    private Text name;
    private BooleanWritable gender;

    AccountWritable() {
        code = new IntWritable();
        name = new Text();
        gender = new BooleanWritable();
    }

    // 把参数类型和类类型相同的构造器，叫复制构造器
    AccountWritable(AccountWritable aw) {
        code = new IntWritable(aw.getCode().get());
        name = new Text(aw.getName().toString());
        gender = new BooleanWritable(aw.getGender().get());
    }

    public void set(IntWritable code, Text name, BooleanWritable gender) {
        this.code = new IntWritable(code.get());
        this.name = new Text(name.toString());
        this.gender = new BooleanWritable(gender.get());
    }

    public int compareTo(AccountWritable o) {
        int comp = this.code.compareTo(o.code);
        if (comp != 0) {
            return comp;
        } else {
            comp = this.name.compareTo(o.name);
            if (comp != 0) {
                return comp;
            } else {
                comp = this.gender.compareTo(o.gender);
                if (comp != 0) {
                    return comp;
                } else {
                    return 0;
                }
            }
        }
    }


    public void write(DataOutput out) throws IOException {
        code.write(out);
        name.write(out);
        gender.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        code.readFields(in);
        name.readFields(in);
        gender.readFields(in);
    }

    //实现一个比较器
    static class DiyComparator
            implements RawComparator<AccountWritable> {

        private IntWritable.Comparator ic =
                new Comparator();
        private Text.Comparator tc =
                new Text.Comparator();
        private BooleanWritable.Comparator bc =
                new BooleanWritable.Comparator();

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // code被序列化后在b1和b2数组中的起始位置以及字节长度
            int firstLength = 4;
            int secondLength = 4;

            int firstStart = s1;
            int secondStart = s2;

            int firstOffset = 0;
            int secondOffset = 0;

            // 比较字节流中的code部分
            int comp = ic.compare(
                    b1, firstStart, firstLength,
                    b2, secondStart, secondLength);
            if (comp != 0) {
                return comp;
            } else {
                try {
                    // 获取记录字符串的起始位置
                    firstStart = firstStart + firstLength;
                    secondStart = secondStart + secondLength;
                    // 获取记录字符串长度的VIntWritable的值的长度，被称为offset
                    firstOffset = WritableUtils.decodeVIntSize(b1[firstStart]);
                    secondOffset = WritableUtils.decodeVIntSize(b2[secondStart]);
                    // 获取字符串的长度
                    firstLength = readLengthValue(b1, firstStart);
                    secondLength = readLengthValue(b2, secondStart);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // 比较字节流中的name部分
                comp = tc.compare(b1, firstStart + firstOffset, firstLength, b2, secondStart + secondOffset, secondLength);
                if (comp != 0) {
                    return comp;
                } else {
                    firstStart += (firstOffset + firstLength);
                    secondStart += (secondOffset + secondLength);
                    firstLength = 1;
                    secondLength = 1;
                    // 比较字节流中的gender部分
                    return bc.compare(b1, firstStart, firstLength, b2, secondStart, secondLength);
                }
            }
        }

        private int readLengthValue(
                byte[] bytes, int start) throws IOException {
            DataInputStream dis =
                    new DataInputStream(
                            new ByteArrayInputStream(
                                    bytes, start, WritableUtils.decodeVIntSize(bytes[start])));
            VIntWritable viw = new VIntWritable();
            viw.readFields(dis);
            return viw.get();
        }

        public int compare(AccountWritable o1, AccountWritable o2) {
            ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
            DataOutputStream dos1 = new DataOutputStream(baos1);

            ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
            DataOutputStream dos2 = new DataOutputStream(baos2);

            try {
                o1.write(dos1);
                o2.write(dos2);

                dos1.close();
                dos2.close();

                byte[] b1 = baos1.toByteArray();
                byte[] b2 = baos2.toByteArray();

                return compare(b1, 0, b1.length, b2, 0, b2.length);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }

    public IntWritable getCode() {
        return code;
    }

    public void setCode(IntWritable code) {
        this.code = code;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public BooleanWritable getGender() {
        return gender;
    }

    public void setGender(BooleanWritable gender) {
        this.gender = gender;
    }
}
