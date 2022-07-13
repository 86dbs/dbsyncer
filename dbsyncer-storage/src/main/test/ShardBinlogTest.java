import com.google.protobuf.ByteString;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.storage.binlog.AbstractBinlogRecorder;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.binlog.proto.EventEnum;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.query.Option;
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
        for (int i = 1; i <= 10; i++) {
            BinlogMessage message = genMessage("123456", i + "");
            Document doc = new Document();
            doc.add(new LongPoint("id", i));
            doc.add(new StoredField("id", i));
            doc.add(new NumericDocValuesField("id", i));

            doc.add(new IntPoint("s", 0));
            doc.add(new StoredField("s", 0));
            doc.add(new NumericDocValuesField("s", 0));

            BytesRef ref = new BytesRef(message.toByteArray());
            doc.add(new BinaryDocValuesField("content", ref));
            doc.add(new StoredField("content", ref));
            list.add(doc);

            if (i % 1000 == 0) {
                shard.insertBatch(list);
                list.clear();
            }
        }

        if (!list.isEmpty()) {
            shard.insertBatch(list);
        }
        check();
        Document doc = new Document();
        doc.add(new LongPoint("id", 1));
        doc.add(new StoredField("id", 1));
        doc.add(new NumericDocValuesField("id", 1));

        doc.add(new IntPoint("s", 1));
        doc.add(new StoredField("s", 1));
        doc.add(new NumericDocValuesField("s", 1));
        shard.update(new Term("id", "1"), doc);

        Option option = new Option(new MatchAllDocsQuery());
        option.addIndexFieldResolverEnum("id", IndexFieldResolverEnum.LONG);
        option.addIndexFieldResolverEnum("s", IndexFieldResolverEnum.INT);
        option.addIndexFieldResolverEnum("content", IndexFieldResolverEnum.BINARY);
        Paging paging = shard.query(option, 1, 10001, null);
        List<Map> maps = (List<Map>) paging.getData();
        for (Map m : maps) {
            Long id = (Long) m.get("id");
            Long tid = (Long) m.get("tid");
            Integer s = (Integer) m.get("s");
            BytesRef ref = (BytesRef) m.get("content");
            BinlogMessage message = BinlogMessage.parseFrom(ref.bytes);
            Map<String, ByteString> rowMap = message.getData().getRowMap();
            logger.info("id:{}, tid:{}, s:{}, message:{}", id, tid, s, rowMap.get("name").toStringUtf8());
        }

        logger.info("总条数：{}", paging.getTotal());
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
                ByteString bytes = serializeValue(v);
                if (null != bytes) {
                    builder.putRow(k, bytes);
                }
            }
        });

        BinlogMessage build = BinlogMessage.newBuilder()
                .setTableGroupId(tableGroupId)
                .setEvent(EventEnum.UPDATE)
                .setData(builder.build())
                .build();
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