package com.test;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EventTimeBaseSource extends Kafka010TableSource {

    private List<RowtimeAttributeDescriptor> list = new ArrayList<>();

    public EventTimeBaseSource(TableSchema schema, String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
        super(schema, topic, properties, deserializationSchema);
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return list;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        String[] names = new String[]{"name", "age", "sid"};
        TypeInformation[] types = new TypeInformation[]{Types.STRING(), Types.INT(), Types.STRING()};
        return Types.ROW(names, types);
    }

    public void setRowtimeAttributeDescriptor(List<RowtimeAttributeDescriptor> list) {
        this.list = list;
    }
}
