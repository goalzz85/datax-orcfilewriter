package com.alibaba.datax.plugin.writer.orcfilewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OrcFileHelper {
    private static final Logger LOG = LoggerFactory.getLogger(OrcFileHelper.class);
    public static void writeRowToBatch(VectorizedRowBatch rowBatch, List<ColumnType> columnTypeList, List<Record> recordList) {
        if (rowBatch == null || columnTypeList == null || columnTypeList.isEmpty() || recordList == null) {
            return;
        }

        for (int i = 0; i < recordList.size(); i++) {
            Record record = recordList.get(i);
            for (int c = 0; c < columnTypeList.size(); c++) {
                Column column = record.getColumn(c);
                ColumnType columnType = columnTypeList.get(c);
                ColumnVector columnVector = rowBatch.cols[c];
                writeColumn(columnVector, column, columnType.toTypeDesc(), i);
            }
            rowBatch.size++;
        }
    }

    private static void writeColumn(ColumnVector columnVector, Column column, TypeDescription typeDescription, int index) {
        //LOG.info(String.format("typeis: %s, data is: %s", column.getType().toString(), column.asString()));
        if (column.getRawData() == null) {
            columnVector.isNull[index] = true;
            columnVector.noNulls = false;
            return;
        }
        if (Column.Type.INT.equals(column.getType())
                || Column.Type.LONG.equals(column.getType())
                || Column.Type.BOOL.equals(column.getType())) {
            ((LongColumnVector) columnVector).vector[index] = column.asLong();
        } else if (Column.Type.DATE.equals(column.getType())) {
            if (typeDescription.getCategory().equals(TypeDescription.Category.DATE)) {
                long days = column.asLong() / (86400 * 1000);
                ((LongColumnVector) columnVector).vector[index] = days;
            } else {
                    //MySQL Datetime在5.x的mysql driver也会转换为timestamp类型然后到datax的date类型
                    //时间转化为字符串的mysql datetime按照本地时区转化为timestamp
                    //其它Reader可能有不同的做法
                    //但因为DateColumn的datetime子类型传参为timestamp，默认直接转换为orc的timestamp应该没有问题
                    // TODO 这里没有处理TIME子类型，因为ORC没有该类型，就按时间戳处理
                    ((TimestampColumnVector) columnVector).time[index] = column.asLong();
            }
        } else if (Column.Type.BYTES.equals(column.getType())) {
            ((BytesColumnVector) columnVector).setVal(index, column.asBytes());
        } else if (Column.Type.DOUBLE.equals(column.getType())) {
            if (typeDescription.getCategory().equals(TypeDescription.Category.DECIMAL)) {
                ((DecimalColumnVector) columnVector).vector[index] = new HiveDecimalWritable(column.asString());
            } else {
                ((DoubleColumnVector) columnVector).vector[index] = column.asDouble();
            }
        } else if (Column.Type.NULL.equals(column.getType())
                || Column.Type.BAD.equals(column.getType())) {
            columnVector.isNull[index] = true;
            columnVector.noNulls = false;
        } else if (Column.Type.STRING.equals(column.getType())) {
            ((BytesColumnVector) columnVector).setVal(index, column.asBytes());
        }
    }
}
