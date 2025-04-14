/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.IOUtils;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-12 20:29
 */
public class Shard {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final File indexPath;

    private final Directory directory;

    private final Analyzer analyzer;

    private IndexWriter indexWriter;

    private IndexReader indexReader;

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
            reopen();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public void insertBatch(List<Document> docs) {
        execute(docs, () -> indexWriter.addDocuments(docs));
    }

    public void update(Term term, Document doc) {
        if (null != term) {
            execute(doc, () -> indexWriter.updateDocument(term, doc));
        }
    }

    public void deleteBatch(Term... terms) {
        if (null != terms) {
            execute(terms, () -> indexWriter.deleteDocuments(terms));
        }
    }

    public void delete(Query query) {
        try {
            indexWriter.deleteDocuments(query);
            indexWriter.flush();
            indexWriter.commit();
            indexWriter.forceMergeDeletes();
            indexWriter.deleteUnusedFiles();
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
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
            r = new ConcurrentHashMap();
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
                        logger.error(e.getLocalizedMessage(), e);
                    }
                }

                // 解析value类型
                r.put(f.name(), option.getFieldResolver(f.name()).getValue(f));
            }
            list.add(r);
        }
        return list;
    }

    private void execute(Object value, Callback callback) {
        if (value == null) {
            return;
        }

        if (indexWriter.isOpen()) {
            try {
                doExecute(callback);
            } catch (IOException e) {
                // 程序异常导致文件锁未关闭 java.nio.channels.ClosedChannelException
                logger.error(String.format("索引异常：%s", indexPath.getAbsolutePath()), e);
            }
            return;
        }

        // 索引异常关闭
        try {
            reopen();
            doExecute(callback);
        } catch (IOException e) {
            // 重试失败打印异常数据
            logger.error(value.toString());
        }
    }

    private void doExecute(Callback callback) throws IOException {
        callback.execute();
        indexWriter.flush();
        indexWriter.commit();
    }

    private void reopen() throws IOException {
        Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        if (writeLock != null) {
            IOUtils.close(writeLock); // release write lock
        }
        // 创建索引写入配置
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        // 默认32M, 减少合并次数
        config.setRAMBufferSizeMB(32);
        // 创建索引写入对象
        indexWriter = new IndexWriter(directory, config);
        // 创建索引的读取器
        indexReader = DirectoryReader.open(indexWriter);
    }

    public Analyzer getAnalyzer() {
        return analyzer;
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