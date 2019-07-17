package com.test.typeinfo;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhangzhiqiang
 * @date 2019/7/17 15:21
 */
public class TypeInfomationRows {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        TypeInformation<Row> row1 = TypeInformation.of(Row.class);
        TypeInformation<Row> row2 = TypeInformation.of(new TypeHint<Row>() {
        });


        RowTypeInfo row3 = new RowTypeInfo();

        TypeInformation<?>[] types = new TypeInformation[]{Types.STRING, Types.STRING};
        String[] names = new String[]{"id", "name"};
        RowTypeInfo row4 = new RowTypeInfo(types, names);

        TypeInformation<Row> row5 = Types.ROW();
        TypeInformation<Row> row6 = Types.ROW(Types.STRING, Types.STRING);


        TypeInformation<Row> row7 = TypeInformation.of(new TypeHint<Row>() {
            @Override
            public TypeInformation<Row> getTypeInfo() {
                return row4;
            }
        });




        env.execute("TypeInfomationRows");
    }
}
