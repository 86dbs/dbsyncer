/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.util.BytesRef;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.RandomUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.dbsyncer.storage.lucene.Option;
import org.dbsyncer.storage.lucene.Shard;
import org.dbsyncer.storage.util.BinlogMessageUtil;
import org.dbsyncer.storage.util.DocumentUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LuceneFactoryTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Shard shard;

    private SnowflakeIdWorker snowflakeIdWorker = new SnowflakeIdWorker();

    @Before
    public void setUp() throws IOException {
        shard = new Shard("target/indexDir/");
    }

    @After
    public void tearDown() throws IOException {
        shard.deleteAll();
    }

    @Test
    public void testConcurrentUpdate() throws InterruptedException {
        // 模拟并发
        final int threadSize = 100;
        final ExecutorService pool = Executors.newFixedThreadPool(threadSize);
        final CyclicBarrier barrier = new CyclicBarrier(threadSize);
        final CountDownLatch latch = new CountDownLatch(threadSize);
        for (int i = 0; i < threadSize; i++) {
            final int k = i + 1;
            pool.submit(() -> {
                try {
                    barrier.await();

                    // 模拟操作
                    System.out.println(String.format("%s:%s", Thread.currentThread().getName(), k));

                    List<Document> docs = new ArrayList<>();
                    docs.add(DocumentUtil.convertData2Doc(createMap(k)));
                    shard.insertBatch(docs);

                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                } catch (BrokenBarrierException e) {
                    logger.error(e.getMessage());
                } catch (Exception e) {
                    logger.error(e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            logger.info("try to shutdown");
            pool.shutdown();
            check();
            Sort sort = new Sort(new SortField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortField.Type.LONG, true),
                    new SortField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, SortField.Type.LONG, true));
            Option option = new Option();
            option.setQuery(new MatchAllDocsQuery());
            Paging paging = shard.query(option, 1, 20, sort);
            if (!CollectionUtils.isEmpty(paging.getData())) {
                List<Map> data = (List<Map>) paging.getData();
                data.stream().forEach(r -> System.out.println(r));
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        TimeUnit.SECONDS.sleep(3);
        logger.info("test end");
    }

    private Map<String, Object> createMap(int i) {
        Map<String, Object> params = new HashMap();
        params.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
        params.put(ConfigConstant.DATA_SUCCESS, StorageDataStatusEnum.SUCCESS.getValue());
        params.put(ConfigConstant.DATA_EVENT, ConnectorConstant.OPERTION_UPDATE);
        params.put(ConfigConstant.DATA_TABLE_GROUP_ID, "" + i);
        params.put(ConfigConstant.DATA_TARGET_TABLE_NAME, "MY_USER");
        params.put(ConfigConstant.DATA_ERROR, "");
        Map<String, Object> row = new HashMap<>();
        row.put("id", i);
        row.put("name", "中文");
        row.put("tel", "15800001234");
        row.put("update_time", Instant.now().toEpochMilli());
        row.put("remark", "test" + i);
        params.put(ConfigConstant.BINLOG_DATA, BinlogMessageUtil.toBinlogMap(row).toByteArray());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, Instant.now().toEpochMilli());
        return params;
    }

    @Test
    public void testQuery() throws IOException {
        int size = 3;
        List<Document> docs = new ArrayList<>();
        for (int i = size; i > 0; i--) {
            Document doc = new Document();
            doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
            doc.add(new StringField("name", "中文" + i, Field.Store.YES));
            doc.add(new TextField("content", "这是一串很长长长长长长长的文本", Field.Store.YES));

            // 创建索引
            int age = RandomUtil.nextInt(0, 50);
            doc.add(new IntPoint("age", age));
            // 需要存储内容
            doc.add(new StoredField("age", age));
            // 需要排序
            doc.add(new NumericDocValuesField("age", age));
            System.out.println(String.format("id=%s,age:=%s", String.valueOf(i), age));

            // 2020-05-23 12:00:00
            long createTime = 1590206400000L + i;
            doc.add(new LongPoint("createTime", createTime));
            doc.add(new StoredField("createTime", createTime));
            doc.add(new NumericDocValuesField("createTime", createTime));
            docs.add(doc);
        }
        shard.insertBatch(docs);
        // 范围查询 IntPoint.newRangeQuery("id", 1, 100)
        // 集合查询 IntPoint.newSetQuery("id", 2, 3)
        // 单个查询 IntPoint.newExactQuery("id", 3)
        BooleanQuery query = new BooleanQuery.Builder()
                .add(IntPoint.newRangeQuery("age", 1, 100), BooleanClause.Occur.MUST)
                .build();
        Option option = new Option();
        option.setQuery(query);
        Paging paging = shard.query(option, 1, 20, new Sort(new SortField("createTime", SortField.Type.LONG, true)));
        paging.getData().forEach(m -> System.out.println(m));

        // 清空
        shard.deleteAll();
    }

    @Test
    public void testCURD() throws IOException {
        System.out.println("测试前：");
        List<Map> maps = query(new MatchAllDocsQuery());
        maps.forEach(m -> System.out.println(m));
        check();

        // 新增
        Document doc = new Document();
        String id = "100";
        doc.add(new StringField("id", id, Field.Store.YES));
        doc.add(new TextField("content", "这是一款大规模数据处理软件，名字叫做Apache Spark", Field.Store.YES));
        List<Document> docs = new ArrayList<>();
        docs.add(doc);
        shard.insertBatch(docs);
        System.out.println("新增后：");
        maps = query(new MatchAllDocsQuery());
        maps.forEach(m -> System.out.println(m));
        check();

        // 修改
        doc.add(new TextField("content", "这是一款大规模数据处理软件，名字叫做Apache Spark[已修改]", Field.Store.YES));
        shard.update(new Term("id", id), doc);
        System.out.println("修改后：");
        maps = query(new MatchAllDocsQuery());
        maps.forEach(m -> System.out.println(m));
        check();

        // 删除
        shard.deleteBatch(new Term("id", id));
        System.out.println("删除后：");
        maps = query(new MatchAllDocsQuery());
        maps.forEach(m -> System.out.println(m));
        check();

        // 清空
        shard.deleteAll();
    }

    @Test
    public void testBinary() throws IOException {
        System.out.println("测试前：");
        List<Map> maps = query(new MatchAllDocsQuery());
        maps.forEach(m -> System.out.println(m));
        check();

        // 新增
        Document doc = new Document();
        String id = "100";
        doc.add(new StringField("id", id, Field.Store.YES));
        BytesRef bytesRef = new BytesRef("中文".getBytes());
        doc.add(new StoredField("content", bytesRef));
        List<Document> docs = new ArrayList<>();
        docs.add(doc);
        shard.insertBatch(docs);
        System.out.println("新增后：");
        maps = query(new MatchAllDocsQuery());
        maps.forEach(m -> {
            logger.info(String.valueOf(m.get("content")));
        });
        check();

        // 清空
        shard.deleteAll();
    }

    /**
     * 按词条搜索
     * <p>
     * TermQuery是最简单、也是最常用的Query。TermQuery可以理解成为“词条搜索”， 在搜索引擎中最基本的搜索就是在索引中搜索某一词条，而TermQuery就是用来完成这项工作的。
     * 在Lucene中词条是最基本的搜索单位，从本质上来讲一个词条其实就是一个名/值对。 只不过这个“名”是字段名，而“值”则表示字段中所包含的某个关键字。
     *
     * @throws IOException
     */
    @Test
    public void testQueryTerm() throws IOException {
        String searchField = "title";
        //这是一个条件查询的api，用于添加条件
        TermQuery query = new TermQuery(new Term(searchField, "Spark"));

        //执行查询，并打印查询到的记录数
        query(query);
    }

    /**
     * 多条件查询
     * <p>
     * BooleanQuery也是实际开发过程中经常使用的一种Query。 它其实是一个组合的Query，在使用时可以把各种Query对象添加进去并标明它们之间的逻辑关系。 BooleanQuery本身来讲是一个布尔子句的容器，它提供了专门的API方法往其中添加子句，
     * 并标明它们之间的关系，以下代码为BooleanQuery提供的用于添加子句的API接口：
     *
     * @throws IOException
     */
    @Test
    public void testQueryBoolean() throws IOException {

        String searchField1 = "title";
        String searchField2 = "content";
        Query query1 = new TermQuery(new Term(searchField1, "Spark"));
        Query query2 = new TermQuery(new Term(searchField2, "Apache"));
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        // BooleanClause用于表示布尔查询子句关系的类，
        // 包 括：
        // BooleanClause.Occur.MUST，
        // BooleanClause.Occur.MUST_NOT，
        // BooleanClause.Occur.SHOULD。
        // 必须包含,不能包含,可以包含三种.有以下6种组合：
        //
        // 1．MUST和MUST：取得连个查询子句的交集。
        // 2．MUST和MUST_NOT：表示查询结果中不能包含MUST_NOT所对应得查询子句的检索结果。
        // 3．SHOULD与MUST_NOT：连用时，功能同MUST和MUST_NOT。
        // 4．SHOULD与MUST连用时，结果为MUST子句的检索结果,但是SHOULD可影响排序。
        // 5．SHOULD与SHOULD：表示“或”关系，最终检索结果为所有检索子句的并集。
        // 6．MUST_NOT和MUST_NOT：无意义，检索无结果。

        builder.add(query1, BooleanClause.Occur.SHOULD);
        builder.add(query2, BooleanClause.Occur.SHOULD);

        BooleanQuery query = builder.build();

        //执行查询，并打印查询到的记录数
        query(query);
    }

    /**
     * 匹配前缀
     * <p>
     * PrefixQuery用于匹配其索引开始以指定的字符串的文档。就是文档中存在xxx%
     * <p>
     *
     * @throws IOException
     */
    @Test
    public void testQueryPrefix() throws IOException {
        String searchField = "title";
        Term term = new Term(searchField, "Spar");
        Query query = new PrefixQuery(term);

        //执行查询，并打印查询到的记录数
        query(query);
    }

    /**
     * 短语搜索
     * <p>
     * 所谓PhraseQuery，就是通过短语来检索，比如我想查“big car”这个短语， 那么如果待匹配的document的指定项里包含了"big car"这个短语， 这个document就算匹配成功。可如果待匹配的句子里包含的是“big black car”，
     * 那么就无法匹配成功了，如果也想让这个匹配，就需要设定slop， 先给出slop的概念：slop是指两个项的位置之间允许的最大间隔距离
     *
     * @throws IOException
     */
    @Test
    public void testQueryPhrase() throws IOException {

        String searchField = "content";
        String query1 = "apache";
        String query2 = "spark";

        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.add(new Term(searchField, query1));
        builder.add(new Term(searchField, query2));
        builder.setSlop(0);
        PhraseQuery phraseQuery = builder.build();

        //执行查询，并打印查询到的记录数
        query(phraseQuery);
    }

    /**
     * 相近词语搜索
     * <p>
     * FuzzyQuery是一种模糊查询，它可以简单地识别两个相近的词语。
     *
     * @throws IOException
     */
    @Test
    public void testQueryFuzzy() throws IOException {

        String searchField = "content";
        Term t = new Term(searchField, "大规模");
        Query query = new FuzzyQuery(t);

        //执行查询，并打印查询到的记录数
        query(query);
    }

    /**
     * 通配符搜索(IO影响较大，不建议使用)
     * <p>
     * Lucene也提供了通配符的查询，这就是WildcardQuery。 通配符“?”代表1个字符，而“*”则代表0至多个字符。
     *
     * @throws IOException
     */
    @Test
    public void testQueryWildcard() throws IOException {
        String searchField = "content";
        Term term = new Term(searchField, "大*规模");
        Query query = new WildcardQuery(term);

        //执行查询，并打印查询到的记录数
        query(query);
    }

    /**
     * 分词查询
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testQueryParser() throws IOException, ParseException {
        final Analyzer analyzer = shard.getAnalyzer();
        String searchField = "content";

        //指定搜索字段和分析器
        QueryParser parser = new QueryParser(searchField, analyzer);
        //QueryParser queryParser = new MultiFieldQueryParser(new String[]{"title", "content"}, analyzer);

        //用户输入内容
        Query query = parser.parse("Spark");

        //执行查询，并打印查询到的记录数
        query(query);
    }

    /**
     * 高亮处理
     *
     * @throws IOException
     */
    @Test
    public void testHighlighter() throws IOException, ParseException, InvalidTokenOffsetsException {
        final Analyzer analyzer = shard.getAnalyzer();
        final IndexSearcher searcher = shard.getSearcher();

        String searchField = "content";
        String text = "大规模";

        //指定搜索字段和分析器
        QueryParser parser = new QueryParser(searchField, analyzer);

        //用户输入内容
        Query query = parser.parse(text);

        TopDocs topDocs = searcher.search(query, 100);

        // 关键字高亮显示的html标签，需要导入lucene-highlighter-xxx.jar
        SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter("<span style='color:red'>", "</span>");
        Highlighter highlighter = new Highlighter(simpleHTMLFormatter, new QueryScorer(query));

        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {

            //取得对应的文档对象
            Document document = searcher.doc(scoreDoc.doc);

            // 内容增加高亮显示
            TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(document.get("content")));
            String content = highlighter.getBestFragment(tokenStream, document.get("content"));

            System.out.println(content);
        }

    }

    @Test
    public void testAnalyzerDoc() throws IOException {
        // SmartChineseAnalyzer  smartcn分词器 需要lucene依赖 且和lucene版本同步
        Analyzer analyzer = new SmartChineseAnalyzer();

        String text = "Apache Spark 是专为大规模数据处理而设计的快速通用的计算引擎";
        TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(text));
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        try {
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                System.out.println(charTermAttribute.toString());
            }
            tokenStream.end();
        } finally {
            tokenStream.close();
            analyzer.close();
        }
    }

    private List<Map> query(Query query) throws IOException {
        Option option = new Option();
        option.setQuery(query);
        Paging paging = shard.query(option, 1, 20, null);
        return (List<Map>) paging.getData();
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