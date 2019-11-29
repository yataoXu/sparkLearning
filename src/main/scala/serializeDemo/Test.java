package serializeDemo;

public class Test {
    public static void main(String[] args) throws Exception {
        //测试序列化
        Person  person = new Person("zhangsan",27,"main");
        byte[] values = HadoopSerializationUtil.serialize(person);

        //测试反序列化
        Person p = new Person();
        HadoopSerializationUtil.deserialize(p,values);
        System.out.println(p);
    }
}
