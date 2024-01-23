package com.alibaba.datax.plugin.writer.orcfilewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

public class OrcFileWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
        }

        @Override
        public void prepare() {
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig.getString(Key.FILE_NAME);
            File dir = new File(path);

            try {
                if (dir.exists()) {
                    //清理已经存在的文件
                    FilenameFilter filter = new PrefixFileFilter(fileName);
                    File[] filesWithFileNamePrefix = dir.listFiles(filter);
                    if (filesWithFileNamePrefix != null) {
                        for (File eachFile : filesWithFileNamePrefix) {
                            LOG.info(String.format("delete file [%s].",
                                    eachFile.getName()));
                            FileUtils.forceDelete(eachFile);

                        }
                    }
                }
            } catch (NullPointerException npe) {
                throw DataXException
                        .asDataXException(
                                OrcFileWriterErrorCode.Write_FILE_ERROR,
                                String.format("配置的目录清空时出现空指针异常 : [%s]",
                                        path), npe);
            }  catch (IllegalArgumentException iae) {
                throw DataXException.asDataXException(
                        OrcFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您配置的目录参数异常 : [%s]", path));
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        OrcFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限查看目录 : [%s]", path));
            } catch (IOException e) {
                throw DataXException.asDataXException(
                        OrcFileWriterErrorCode.Write_FILE_ERROR,
                        String.format("无法清空目录 : [%s]", path), e);
            }
        }

        private void validateParameter() {
            this.writerSliceConfig
                    .getNecessaryValue(
                            Key.FILE_NAME,
                            OrcFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                    OrcFileWriterErrorCode.REQUIRED_VALUE);

            List<String> columnList = this.writerSliceConfig.getList(Key.COLUMN, String.class);
            if (columnList == null) {
                throw DataXException
                        .asDataXException(
                                OrcFileWriterErrorCode.ILLEGAL_VALUE,
                                "您配置的column: column是一个List<String>对象，请注意不能为空以及数据类型正确");
            }

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                            .asDataXException(
                                    OrcFileWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                                .asDataXException(
                                        OrcFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        OrcFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<>();

            String filePrefix = this.writerSliceConfig
                    .getString(Key.FILE_NAME);

            String path = null;

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration splitedTaskConfig = this.writerSliceConfig.clone();
                String fullFileName = null;
                fullFileName = String.format("%s-%d", filePrefix, i);
                splitedTaskConfig.set(Key.FILE_NAME, fullFileName);

                writerSplitConfigs.add(splitedTaskConfig);
            }
            return writerSplitConfigs;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;

        private String fileName;

        private List<ColumnType> columnTypeList;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(Key.PATH);
            this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME);
            this.columnTypeList = this.getColumnTypeListFromConfig();
        }

        private List<ColumnType> getColumnTypeListFromConfig() {
            List<ColumnType> columnTypeListRet = new ArrayList<>();
            List<Map<String, String>> columnConfigs = new ArrayList<>();

            List<Object> configList = this.writerSliceConfig.getList(Key.COLUMN);
            if (configList == null) {
                return new ArrayList<>();
            }

            for (Object obj : configList) {
                if (!(obj instanceof JSONObject)) {
                    throw DataXException.asDataXException(OrcFileWriterErrorCode.CONFIG_INVALID_EXCEPTION, "Column参数错误");
                }

                JSONObject config = ((JSONObject) obj);
                String columnNameStr = config.getString(Key.COLUMN_NAME);
                String columnTypeStr = config.getString(Key.COLUMN_TYPE);
                if (StringUtils.isEmpty(columnNameStr) || StringUtils.isEmpty(columnTypeStr)) {
                    throw DataXException.asDataXException(OrcFileWriterErrorCode.CONFIG_INVALID_EXCEPTION, "column的name或者type为空");
                }
                ColumnType columnType = new ColumnType(columnNameStr, columnTypeStr);
                columnTypeListRet.add(columnType);
            }

            return columnTypeListRet;
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            String fileFullPath = this.buildFilePath();
            LOG.info(String.format("write to file : [%s]", fileFullPath));

            //构建Schema
            TypeDescription schema = TypeDescription.createStruct();
            for (ColumnType columnType : this.columnTypeList) {
                schema.addField(columnType.getName(), columnType.toTypeDesc());
            }
            org.apache.orc.Writer writer = null;
            try {
                writer = OrcFile.createWriter(new Path(this.buildFilePath()),
                        OrcFile.writerOptions((new org.apache.hadoop.conf.Configuration())).setSchema(schema)
                );
            } catch (IOException e) {
                throw DataXException.asDataXException(OrcFileWriterErrorCode.Write_FILE_ERROR, String.format("创建Orc写入文件错误: %s", this.buildFilePath()));
            }

            int batchSize = 1024;
            VectorizedRowBatch rowBatch = schema.createRowBatch(batchSize);
            List<Record> recordBuffList = new ArrayList<>(batchSize);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                recordBuffList.add(record);
                if (recordBuffList.size() != batchSize) {
                    //写入具体数据
                    OrcFileHelper.writeRowToBatch(rowBatch, this.columnTypeList, recordBuffList);
                    try {
                        writer.addRowBatch(rowBatch);
                    } catch (IOException e) {
                        throw DataXException.asDataXException(OrcFileWriterErrorCode.Write_FILE_ERROR, String.format("写入文件错误: %s", this.buildFilePath()));
                    }
                    recordBuffList.clear();
                    rowBatch.reset();
                }
            }

            try {
                if (!recordBuffList.isEmpty()) {
                    OrcFileHelper.writeRowToBatch(rowBatch, this.columnTypeList, recordBuffList);
                    writer.addRowBatch(rowBatch);
                }

                writer.close();
            } catch (IOException e) {
                throw DataXException.asDataXException(OrcFileWriterErrorCode.Write_FILE_ERROR, String.format("写入文件错误: %s", this.buildFilePath()));
            }
        }




        private String buildFilePath() {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = this.path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = this.path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                    break;
                default:
                    break;
            }
            if (!isEndWithSeparator) {
                this.path = this.path + IOUtils.DIR_SEPARATOR;
            }
            return String.format("%s%s", this.path, this.fileName);
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}
