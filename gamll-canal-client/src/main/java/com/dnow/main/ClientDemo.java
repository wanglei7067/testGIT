package com.dnow.main;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.common.constants.GmallConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class ClientDemo {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop104", 11111),
                "example", null, null);
        canalConnector.connect();
        // 订阅 哪个库，哪些表
        canalConnector.subscribe("dnow.*");

        while (true){

            Message message = canalConnector.get(100);
//            System.out.println(message);

            if (message.getId() == -1L) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            List<CanalEntry.Entry> entryList = message.getEntries();
            // 解析得到的结果
            for (CanalEntry.Entry entry : entryList) {
                if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())){
                    String tableName = entry.getHeader().getTableName();
                    ByteString storeValue = entry.getStoreValue();
                    // 解析数据
                    parseStroeValue(tableName,storeValue);
                }
            }
        }
    }

    private static void parseStroeValue(String tableName, ByteString storeValue) throws InvalidProtocolBufferException {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

        if (CanalEntry.EventType.INSERT.equals(rowChange.getEventType())
            && "order_info".equals(tableName)){
            extracted(rowChange,GmallConstants.KAFKA_TOPIC_ORDER_INFO);
        }

        if (CanalEntry.EventType.INSERT.equals(rowChange.getEventType())
                && "order_detail".equals(tableName)){
            extracted(rowChange,GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }

        if ((CanalEntry.EventType.INSERT.equals(rowChange.getEventType()) || CanalEntry.EventType.UPDATE.equals(rowChange.getEventType()) )
                && "user_info".equals(tableName)){
            extracted(rowChange,GmallConstants.KAFKA_TOPIC_USER_INFO);
        }
    }

    private static void extracted(CanalEntry.RowChange rowChange,String topicName) {
        // N行
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        // 1行
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject object = new JSONObject();
            // 写入后 所有列的集合
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            for (CanalEntry.Column column : afterColumnsList) {
                object.put(column.getName(),column.getValue());
            }
            MyProducer.writeToKafka(topicName, object.toJSONString());
        }
    }
}
