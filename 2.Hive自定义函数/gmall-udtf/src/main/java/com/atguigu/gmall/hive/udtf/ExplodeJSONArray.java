package com.atguigu.gmall.hive.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.json.JSONArray;


public class ExplodeJSONArray extends GenericUDTF {

    private PrimitiveObjectInspector inputOI;

    @Override
    public void close() throws HiveException {

    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        if(argOIs.length!=1){
            throw new UDFArgumentException("explode_json_array函数只能接收1个参数");
        }

        ObjectInspector argOI = argOIs[0];

        if(argOI.getCategory()!=ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("explode_json_array函数只能接收基本数据类型的参数");
        }

        PrimitiveObjectInspector primitiveOI  = (PrimitiveObjectInspector) argOI;
        inputOI=primitiveOI;

        if(primitiveOI.getPrimitiveCategory()!=PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentException("explode_json_array函数只能接收STRING类型的参数");
        }


        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("item");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        Object arg = args[0];
        String jsonArrayStr = PrimitiveObjectInspectorUtils.getString(arg, inputOI);

        JSONArray jsonArray = new JSONArray(jsonArrayStr);

        for (int i = 0; i < jsonArray.length(); i++) {
            String json = jsonArray.getString(i);

            String[] result = {json};

            forward(result);
        }

    }

}