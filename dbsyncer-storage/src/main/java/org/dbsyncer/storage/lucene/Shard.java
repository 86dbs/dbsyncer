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
import org.apache.lucene.store.LockObtainFailedException;
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
        int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                // 尝试获取写锁
                Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
                if (writeLock != null) {
                    IOUtils.close(writeLock); // release write lock
                }
                // 成功获取锁，跳出循环
                break;
            } catch (LockObtainFailedException e) {
                retryCount++;
                // 锁被其他进程持有或残留，尝试清理锁文件
                logger.warn("无法获取 Lucene 写锁 (尝试 {}/{}), 尝试清理锁文件: {}", 
                    retryCount, maxRetries, indexPath.getAbsolutePath());
                
                if (retryCount >= maxRetries) {
                    // 最后一次重试失败，抛出异常
                    throw new IOException("无法获取 Lucene 写锁，已重试 " + maxRetries + " 次。请检查是否有其他进程正在使用索引目录: " + indexPath.getAbsolutePath(), e);
                }
                
                try {
                    // 对于 FSDirectory，锁文件是物理文件，可以直接删除
                    File lockFile = new File(indexPath, IndexWriter.WRITE_LOCK_NAME);
                    if (lockFile.exists()) {
                        boolean deleted = lockFile.delete();
                        if (deleted) {
                            logger.info("已删除残留的 Lucene 锁文件: {}", lockFile.getAbsolutePath());
                            // 删除锁文件后，等待一小段时间让其他进程释放
                            try {
                                Thread.sleep(100); // 等待 100ms
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        } else {
                            logger.warn("无法删除 Lucene 锁文件，可能被其他进程占用: {}", lockFile.getAbsolutePath());
                            // 如果无法删除，等待更长时间
                            try {
                                Thread.sleep(500); // 等待 500ms
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    } else {
                        // 锁文件不存在，但获取锁失败，可能是其他进程正在创建锁
                        logger.warn("锁文件不存在但获取锁失败，等待后重试: {}", indexPath.getAbsolutePath());
                        try {
                            Thread.sleep(200); // 等待 200ms
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } catch (Exception ex) {
                    logger.error("清理 Lucene 锁文件失败: {}", indexPath.getAbsolutePath(), ex);
                    // 继续重试
                }
            }
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