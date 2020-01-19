package org.dbsyncer.storage.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;

public class LuceneFactoryTest {

    private Directory directory;

    private Analyzer analyzer;

    private IndexWriter indexWriter;

    private IndexReader indexReader;

    private IndexSearcher indexSearcher;

    @Before
    public void setUp() throws IOException {
        //索引存放的位置，设置在当前目录中
        directory = FSDirectory.open(Paths.get("target/indexDir/"));

        analyzer = new SmartChineseAnalyzer();

        //创建索引写入配置
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);

        //创建索引写入对象
        indexWriter = new IndexWriter(directory, indexWriterConfig);

        //创建索引的读取器
        indexReader = DirectoryReader.open(indexWriter);

        //创建一个索引的查找器，来检索索引库
        indexSearcher = new IndexSearcher(indexReader);

    }

    @After
    public void tearDown() throws Exception {
        //关闭流
        indexWriter.close();
        indexReader.close();
    }

    /**
     * 执行查询，并打印查询到的记录数
     * @param query
     * @throws IOException
     */
    protected void executeQuery(Query query) throws IOException {

        TopDocs topDocs = indexSearcher.search(query, 100);

        //打印查询到的记录数
        System.out.println("总共查询到" + topDocs.totalHits + "个文档");
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {

            //取得对应的文档对象
            Document document = indexSearcher.doc(scoreDoc.doc);
            System.out.println("id：" + document.get("id"));
            System.out.println("title：" + document.get("title"));
            System.out.println("content：" + document.get("content"));
        }
    }

    /**
     * 分词打印
     *
     * @param analyzer
     * @param text
     * @throws IOException
     */
    protected void printAnalyzerDoc(Analyzer analyzer, String text) throws IOException {

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

    @Test
    public void indexWriterTest() throws IOException {
        long start = System.currentTimeMillis();

        //创建Document对象，存储索引
        Document doc = new Document();

        int id = 1;

        //将字段加入到doc中
        doc.add(new IntPoint("id", id));
        doc.add(new StringField("title", "Spark", Field.Store.YES));
        doc.add(new TextField("content", "Apache Spark 是专为大规模数据处理而设计的快速通用的计算引擎", Field.Store.YES));
        doc.add(new StoredField("id", id));

        //将doc对象保存到索引库中
        indexWriter.addDocument(doc);

        indexWriter.commit();

        long end = System.currentTimeMillis();
        System.out.println("索引花费了" + (end - start) + " 毫秒");
    }

    @Test
    public void updateDocumentTest() throws IOException {
        Document doc = new Document();

        int id = 1;

        doc.add(new IntPoint("id", id));
        doc.add(new StringField("title", "Spark", Field.Store.YES));
        doc.add(new TextField("content", "Apache Spark 是专为大规模数据处理而设计的快速通用的计算引擎", Field.Store.YES));
        doc.add(new StoredField("id", id));

        long count = indexWriter.updateDocument(new Term("id", "1"), doc);
        System.out.println("更新文档:" + count);
        indexWriter.commit();
    }

    @Test
    public void deleteDocumentsTest() throws IOException {

        // 删除title中含有关键词“Spark”的文档
        long count = indexWriter.deleteDocuments(new Term("title", "Spark"));

        //  除此之外IndexWriter还提供了以下方法：
        // DeleteDocuments(Query query):根据Query条件来删除单个或多个Document
        // DeleteDocuments(Query[] queries):根据Query条件来删除单个或多个Document
        // DeleteDocuments(Term term):根据Term来删除单个或多个Document
        // DeleteDocuments(Term[] terms):根据Term来删除单个或多个Document
        // DeleteAll():删除所有的Document

        //使用IndexWriter进行Document删除操作时，文档并不会立即被删除，而是把这个删除动作缓存起来，当IndexWriter.Commit()或IndexWriter.Close()时，删除操作才会被真正执行。

        indexWriter.commit();

        System.out.println("删除完成:" + count);
    }

    /**
     * 按词条搜索
     * <p>
     * TermQuery是最简单、也是最常用的Query。TermQuery可以理解成为“词条搜索”，
     * 在搜索引擎中最基本的搜索就是在索引中搜索某一词条，而TermQuery就是用来完成这项工作的。
     * 在Lucene中词条是最基本的搜索单位，从本质上来讲一个词条其实就是一个名/值对。
     * 只不过这个“名”是字段名，而“值”则表示字段中所包含的某个关键字。
     *
     * @throws IOException
     */
    @Test
    public void termQueryTest() throws IOException {

        String searchField = "title";
        //这是一个条件查询的api，用于添加条件
        TermQuery query = new TermQuery(new Term(searchField, "Spark"));

        //执行查询，并打印查询到的记录数
        executeQuery(query);
    }

    /**
     * 多条件查询
     *
     * BooleanQuery也是实际开发过程中经常使用的一种Query。
     * 它其实是一个组合的Query，在使用时可以把各种Query对象添加进去并标明它们之间的逻辑关系。
     * BooleanQuery本身来讲是一个布尔子句的容器，它提供了专门的API方法往其中添加子句，
     * 并标明它们之间的关系，以下代码为BooleanQuery提供的用于添加子句的API接口：
     *
     * @throws IOException
     */
    @Test
    public void BooleanQueryTest() throws IOException {

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
        executeQuery(query);
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
    public void prefixQueryTest() throws IOException {
        String searchField = "title";
        Term term = new Term(searchField, "Spar");
        Query query = new PrefixQuery(term);

        //执行查询，并打印查询到的记录数
        executeQuery(query);
    }

    /**
     * 短语搜索
     * <p>
     * 所谓PhraseQuery，就是通过短语来检索，比如我想查“big car”这个短语，
     * 那么如果待匹配的document的指定项里包含了"big car"这个短语，
     * 这个document就算匹配成功。可如果待匹配的句子里包含的是“big black car”，
     * 那么就无法匹配成功了，如果也想让这个匹配，就需要设定slop，
     * 先给出slop的概念：slop是指两个项的位置之间允许的最大间隔距离
     *
     * @throws IOException
     */
    @Test
    public void phraseQueryTest() throws IOException {

        String searchField = "content";
        String query1 = "apache";
        String query2 = "spark";

        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.add(new Term(searchField, query1));
        builder.add(new Term(searchField, query2));
        builder.setSlop(0);
        PhraseQuery phraseQuery = builder.build();

        //执行查询，并打印查询到的记录数
        executeQuery(phraseQuery);
    }

    /**
     * 相近词语搜索
     * <p>
     * FuzzyQuery是一种模糊查询，它可以简单地识别两个相近的词语。
     *
     * @throws IOException
     */
    @Test
    public void fuzzyQueryTest() throws IOException {

        String searchField = "content";
        Term t = new Term(searchField, "大规模");
        Query query = new FuzzyQuery(t);

        //执行查询，并打印查询到的记录数
        executeQuery(query);
    }

    /**
     * 通配符搜索(IO影响较大，不建议使用)
     * <p>
     * Lucene也提供了通配符的查询，这就是WildcardQuery。
     * 通配符“?”代表1个字符，而“*”则代表0至多个字符。
     *
     * @throws IOException
     */
    @Test
    public void wildcardQueryTest() throws IOException {
        String searchField = "content";
        Term term = new Term(searchField, "大*规模");
        Query query = new WildcardQuery(term);

        //执行查询，并打印查询到的记录数
        executeQuery(query);
    }

    /**
     * 分词查询
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void queryParserTest() throws IOException, ParseException {

        String searchField = "content";

        //指定搜索字段和分析器
        QueryParser parser = new QueryParser(searchField, analyzer);
        //QueryParser queryParser = new MultiFieldQueryParser(new String[]{"title", "content"}, analyzer);

        //用户输入内容
        Query query = parser.parse("Spark");

        //执行查询，并打印查询到的记录数
        executeQuery(query);
    }

    /**
     * 高亮处理
     *
     * @throws IOException
     */
    @Test
    public void HighlighterTest() throws IOException, ParseException, InvalidTokenOffsetsException {

        String searchField = "content";
        String text = "Apache Spark 大规模数据处理";

        //指定搜索字段和分析器
        QueryParser parser = new QueryParser(searchField, analyzer);

        //用户输入内容
        Query query = parser.parse(text);

        TopDocs topDocs = indexSearcher.search(query, 100);

        // 关键字高亮显示的html标签，需要导入lucene-highlighter-xxx.jar
        SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter("<span style='color:red'>", "</span>");
        Highlighter highlighter = new Highlighter(simpleHTMLFormatter, new QueryScorer(query));

        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {

            //取得对应的文档对象
            Document document = indexSearcher.doc(scoreDoc.doc);

            // 内容增加高亮显示
            TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(document.get("content")));
            String content = highlighter.getBestFragment(tokenStream, document.get("content"));

            System.out.println(content);
        }

    }

    /**
     * IKAnalyzer  中文分词器
     * SmartChineseAnalyzer  smartcn分词器 需要lucene依赖 且和lucene版本同步
     *
     * @throws IOException
     */
    @Test
    public void AnalyzerTest() throws IOException {

        String text = "Apache Spark 是专为大规模数据处理而设计的快速通用的计算引擎";
        analyzer = new SmartChineseAnalyzer();
        printAnalyzerDoc(analyzer, text);
    }

}