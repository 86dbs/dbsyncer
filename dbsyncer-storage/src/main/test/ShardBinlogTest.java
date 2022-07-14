import com.google.protobuf.ByteString;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.storage.binlog.AbstractBinlogRecorder;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.EventEnum;
import org.dbsyncer.storage.constant.BinlogConstant;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.query.Option;
import org.dbsyncer.storage.util.BinlogMessageUtil;
import org.dbsyncer.storage.util.ParamsUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/18 23:46
 */
public class ShardBinlogTest extends AbstractBinlogRecorder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Shard shard;

    @Before
    public void init() throws IOException {
        shard = new Shard("target/indexDir/");
    }

    @After
    public void close() throws IOException {
        shard.deleteAll();
    }

    @Test
    public void testBinlogMessage() throws IOException, InterruptedException {
        mockData(1);

        // 查询[待处理] 或 [处理中 且 处理超时]
        List<Map> maps = queryReadyAndProcess();
        logger.info("总条数：{}", maps.size());
        TimeUnit.SECONDS.sleep(1);
        markProcessing(maps);
        logger.info("标记处理中");

        // 模拟新记录
        TimeUnit.SECONDS.sleep(1);
        mockData(6);

        maps = queryReadyAndProcess();
        logger.info("【待处理】和【处理中 且 处理超时】总条数：{}", maps.size());

        logger.info("模拟处理超时，等待10s");
        TimeUnit.SECONDS.sleep(10);

        maps = queryReadyAndProcess();
        logger.info("【待处理】和【处理中 且 处理超时】总条数：{}", maps.size());
    }

    private void markProcessing(List<Map> maps) {
        long updateTime = Instant.now().toEpochMilli();
        maps.forEach(row -> {
            String id = (String) row.get(BinlogConstant.BINLOG_ID);
            BytesRef ref = (BytesRef) row.get(BinlogConstant.BINLOG_CONTENT);
            try {
                shard.update(new Term(BinlogConstant.BINLOG_ID, String.valueOf(id)), ParamsUtil.convertBinlog2Doc(id, BinlogConstant.PROCESSING, ref, updateTime));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void mockData(int i) throws IOException {
        List<Document> list = new ArrayList<>();
        long now = Instant.now().toEpochMilli();
        int size = i + 5;
        while (i < size){
            BinlogMessage message = genMessage("123456", i + "");
            BytesRef bytes = new BytesRef(message.toByteArray());
            list.add(ParamsUtil.convertBinlog2Doc(String.valueOf(i), BinlogConstant.READY, bytes, now));

            if (i % 1000 == 0) {
                shard.insertBatch(list);
                list.clear();
            }
            i++;
        }

        if (!list.isEmpty()) {
            shard.insertBatch(list);
        }
        check();
    }

    private List<Map> queryReadyAndProcess() throws IOException {
        long lastTime = Timestamp.valueOf(LocalDateTime.now().minusSeconds(5)).getTime();
        BooleanQuery filter1 = new BooleanQuery.Builder()
                .add(IntPoint.newExactQuery(BinlogConstant.BINLOG_STATUS, BinlogConstant.READY), BooleanClause.Occur.MUST)
                .build();
        BooleanQuery filter2 = new BooleanQuery.Builder()
                .add(IntPoint.newExactQuery(BinlogConstant.BINLOG_STATUS, BinlogConstant.PROCESSING), BooleanClause.Occur.MUST)
                .add(LongPoint.newRangeQuery(BinlogConstant.BINLOG_TIME, Long.MIN_VALUE, lastTime), BooleanClause.Occur.MUST)
                .build();
        BooleanQuery query = new BooleanQuery.Builder()
                .add(filter1, BooleanClause.Occur.SHOULD)
                .add(filter2, BooleanClause.Occur.SHOULD)
                .build();
        return query(new Option(query));
    }

    private List<Map> query(Option option) throws IOException {
        option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_ID, IndexFieldResolverEnum.STRING);
        option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_STATUS, IndexFieldResolverEnum.INT);
        option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_CONTENT, IndexFieldResolverEnum.BINARY);
        option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_TIME, IndexFieldResolverEnum.LONG);
        Sort sort = new Sort(new SortField(BinlogConstant.BINLOG_TIME, SortField.Type.LONG));
        Paging paging = shard.query(option, 1, 10001, sort);
        List<Map> maps = (List<Map>) paging.getData();
        for (Map m : maps) {
            String id = (String) m.get(BinlogConstant.BINLOG_ID);
            Integer s = (Integer) m.get(BinlogConstant.BINLOG_STATUS);
            BytesRef ref = (BytesRef) m.get(BinlogConstant.BINLOG_CONTENT);
            Long t = (Long) m.get(BinlogConstant.BINLOG_TIME);
            BinlogMessage message = BinlogMessage.parseFrom(ref.bytes);
            Map<String, ByteString> rowMap = message.getData().getRowMap();
            String timestamp = DateFormatUtil.timestampToString(new Timestamp(t));
            logger.info("t:{}, id:{}, s:{}, message:{}", timestamp, id, s, rowMap.get("name").toStringUtf8());
        }
        return maps;
    }

    private BinlogMessage genMessage(String tableGroupId, String key) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1L);
        data.put("name", key + "中文");
        data.put("age", 88);
        data.put("bd", new BigDecimal(88));
        data.put("sex", 1);
        data.put("f", 88.88f);
        data.put("d", 999.99d);
        data.put("b", true);
        short ss = 32767;
        data.put("ss", ss);
        data.put("bytes", "中文666".getBytes(Charset.defaultCharset()));
        data.put("create_date", new Date(Timestamp.valueOf(LocalDateTime.now()).getTime()));
        data.put("update_time", Timestamp.valueOf(LocalDateTime.now()).getTime());

        BinlogMap.Builder builder = BinlogMap.newBuilder();
        data.forEach((k, v) -> {
            if (null != v) {
                ByteString bytes = BinlogMessageUtil.serializeValue(v);
                if (null != bytes) {
                    builder.putRow(k, bytes);
                }
            }
        });

        BinlogMessage build = BinlogMessage.newBuilder().setTableGroupId(tableGroupId).setEvent(EventEnum.UPDATE).setData(builder.build()).build();
        return build;
    }

    @Override
    public Queue getQueue() {
        return null;
    }

    @Override
    public int getQueueCapacity() {
        return 0;
    }

    @Override
    protected Object deserialize(String messageId, BinlogMessage message) {
        return null;
    }

    private void check() throws IOException {
        final IndexSearcher searcher = shard.getSearcher();
        IndexReader reader = searcher.getIndexReader();
        // 通过reader可以有效的获取到文档的数量
        // 有效的索引文档
        System.out.println("有效的索引文档:" + reader.numDocs());
        // 总共的索引文档
        System.out.println("总共的索引文档:" + reader.maxDoc());
        // 删掉的索引文档，其实不恰当，应该是在回收站里的索引文档
        System.out.println("删掉的索引文档:" + reader.numDeletedDocs());
    }

}