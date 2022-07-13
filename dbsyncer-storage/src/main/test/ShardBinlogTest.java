import com.google.protobuf.ByteString;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.dbsyncer.common.model.Paging;
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
    public void testBinlogMessage() throws IOException {
        List<Document> list = new ArrayList<>();
        long now = Instant.now().toEpochMilli();
        for (int i = 1; i <= 10; i++) {
            BinlogMessage message = genMessage("123456", i + "");
            BytesRef bytes = new BytesRef(message.toByteArray());
            list.add(ParamsUtil.convertBinlog2Doc(String.valueOf(i), BinlogConstant.READY, bytes, now));

            if (i % 1000 == 0) {
                shard.insertBatch(list);
                list.clear();
            }
        }

        if (!list.isEmpty()) {
            shard.insertBatch(list);
        }
        check();

        BooleanQuery query = new BooleanQuery.Builder().add(IntPoint.newSetQuery(BinlogConstant.BINLOG_STATUS, BinlogConstant.READY), BooleanClause.Occur.MUST).build();
        List<Map> maps = query(new Option(query));
        // 更新状态为处理中
        maps.forEach(row -> {
            String id = (String) row.get(BinlogConstant.BINLOG_ID);
            BytesRef ref = (BytesRef) row.get(BinlogConstant.BINLOG_CONTENT);
            try {
                shard.update(new Term(BinlogConstant.BINLOG_ID, String.valueOf(id)), ParamsUtil.convertBinlog2Doc(id, BinlogConstant.PROCESSING, ref, now));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // 查询【待处理】
        query = new BooleanQuery.Builder()
                .add(IntPoint.newSetQuery(BinlogConstant.BINLOG_STATUS, BinlogConstant.READY), BooleanClause.Occur.MUST)
                .build();
        maps = query(new Option(query));
        logger.info("【待处理】总条数：{}", maps.size());

        // 查询【待处理】和【处理中】
        query = new BooleanQuery.Builder()
                .add(IntPoint.newSetQuery(BinlogConstant.BINLOG_STATUS, BinlogConstant.READY, BinlogConstant.PROCESSING), BooleanClause.Occur.MUST)
                .build();
        maps = query(new Option(query));
        logger.info("【待处理】和【处理中】总条数：{}", maps.size());
    }

    private List<Map> query(Option option) throws IOException {
        option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_STATUS, IndexFieldResolverEnum.INT);
        option.addIndexFieldResolverEnum(BinlogConstant.BINLOG_CONTENT, IndexFieldResolverEnum.BINARY);
        Paging paging = shard.query(option, 1, 10001, null);
        List<Map> maps = (List<Map>) paging.getData();
        for (Map m : maps) {
            String id = (String) m.get(BinlogConstant.BINLOG_ID);
            Integer s = (Integer) m.get(BinlogConstant.BINLOG_STATUS);
            BytesRef ref = (BytesRef) m.get(BinlogConstant.BINLOG_CONTENT);
            BinlogMessage message = BinlogMessage.parseFrom(ref.bytes);
            Map<String, ByteString> rowMap = message.getData().getRowMap();
            logger.info("id:{}, s:{}, message:{}", id, s, rowMap.get("name").toStringUtf8());
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