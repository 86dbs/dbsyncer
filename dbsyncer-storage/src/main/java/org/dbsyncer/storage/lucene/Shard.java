package org.dbsyncer.storage.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/12 20:29
 */
public class Shard {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private File indexPath;

    private Directory directory;

    private Analyzer analyzer;

    private IndexWriter indexWriter;

    private IndexReader indexReader;

    private IndexWriterConfig config;

    private final Object LOCK = new Object();

    private static final int MAX_SIZE = 10000;

    public Shard(String path) {
        try {
            // 索引存放的位置，设置在当前目录中
            Path dir = Paths.get(path);
            indexPath = new File(dir.toUri());
            directory = FSDirectory.open(dir);
            // 分词器
            analyzer = new SmartChineseAnalyzer();
            // 创建索引写入配置
            config = new IndexWriterConfig(analyzer);
            // 默认32M, 减少合并次数
            config.setRAMBufferSizeMB(32);
            // 创建索引写入对象
            indexWriter = new IndexWriter(directory, config);
            // 创建索引的读取器
            indexReader = DirectoryReader.open(indexWriter);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public void insert(Document doc) throws IOException {
        execute(doc, () -> indexWriter.addDocument(doc));
    }

    public void insertBatch(List<Document> docs) throws IOException {
        execute(docs, () -> indexWriter.addDocuments(docs));
    }

    public void update(Term term, Document doc) throws IOException {
        if (null != term) {
            execute(doc, () -> indexWriter.updateDocument(term, doc));
        }
    }

    public void delete(Term term) throws IOException {
        if (null != term) {
            execute(term, () -> indexWriter.deleteDocuments(term));
        }
    }

    public void deleteBatch(Term... terms) throws IOException {
        if (null != terms) {
            execute(terms, () -> indexWriter.deleteDocuments(terms));
        }
    }

    public void deleteAll() {
        // Fix Bug: this IndexReader is closed. 直接删除文件
        try {
            close();
            directory.close();
            FileUtils.deleteDirectory(indexPath);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public void close() throws IOException {
        indexWriter.commit();
        indexReader.close();
        indexWriter.close();
    }

    public IndexSearcher getSearcher() throws IOException {
        // 复用索引读取器
        IndexReader changeReader = DirectoryReader.openIfChanged((DirectoryReader) indexReader, indexWriter, true);
        if (null != changeReader) {
            indexReader.close();
            indexReader = null;
            synchronized (LOCK) {
                if (null == indexReader) {
                    indexReader = changeReader;
                }
            }
        }
        return new IndexSearcher(indexReader);
    }

    public Paging query(Option option, int pageNum, int pageSize, Sort sort) throws IOException {
        final IndexSearcher searcher = getSearcher();
        final TopDocs topDocs = getTopDocs(searcher, option.getQuery(), MAX_SIZE, sort);
        Paging paging = new Paging(pageNum, pageSize);
        paging.setTotal(topDocs.totalHits.value);
        if (option.isQueryTotal()) {
            return paging;
        }

        List<Map> data = search(searcher, topDocs, option, pageNum, pageSize);
        paging.setData(data);
        return paging;
    }

    private TopDocs getTopDocs(IndexSearcher searcher, Query query, int maxSize, Sort sort) throws IOException {
        if (null != sort) {
            return searcher.search(query, maxSize, sort);
        }
        return searcher.search(query, maxSize);
    }

    /**
     * 执行查询
     *
     * @param searcher
     * @param topDocs
     * @param option
     * @param pageNum
     * @param pageSize
     * @throws IOException
     */
    private List<Map> search(IndexSearcher searcher, TopDocs topDocs, Option option, int pageNum, int pageSize) throws IOException {
        ScoreDoc[] docs = topDocs.scoreDocs;
        int total = docs.length;
        int begin = (pageNum - 1) * pageSize;
        int end = pageNum * pageSize;

        // 判断边界
        begin = begin > total ? total : begin;
        end = end > total ? total : end;

        List<Map> list = new ArrayList<>();
        Document doc = null;
        Map r = null;
        IndexableField f = null;
        Iterator<IndexableField> iterator = null;
        while (begin < end) {
            // 取得对应的文档对象
            doc = searcher.doc(docs[begin++].doc);
            iterator = doc.iterator();
            r = new LinkedHashMap<>();
            while (iterator.hasNext()) {
                f = iterator.next();

                // 开启高亮
                if (option.isEnableHighLightSearch()) {
                    try {
                        final String key = f.name();
                        if (option.getHighLightKeys().contains(key)) {
                            String content = doc.get(key);
                            TokenStream tokenStream = analyzer.tokenStream("", content);
                            content = option.getHighlighter().getBestFragment(tokenStream, content);
                            r.put(key, content);
                            continue;
                        }
                    } catch (InvalidTokenOffsetsException e) {
                        e.printStackTrace();
                    }
                }

                // 解析value类型
                r.put(f.name(), option.getIndexFieldResolver(f.name()).getValue(f));
            }
            list.add(r);
        }
        return list;
    }

    private void execute(Object value, Callback callback) throws IOException {
        if (null != value && indexWriter.isOpen()) {
            callback.execute();
            indexWriter.flush();
            indexWriter.commit();
            return;
        }
        logger.error(value.toString());
    }

    interface Callback {

        /**
         * 索引回执
         *
         * @throws IOException
         */
        void execute() throws IOException;
    }

}