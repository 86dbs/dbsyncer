/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Lucene 分片：写路径仅变更 IndexWriter；commit / deleteUnusedFiles / refresh 由 {@link org.dbsyncer.storage.impl.DiskStorageService} 定时托管，
 * 查询与关闭前会主动 commit，保证读己之写与优雅停机。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019-11-12 20:29
 */
public class Shard {

    private static final int MAX_SIZE = 10000;

    /** 配置索引体量小，降低 Writer 缓冲，减少 flush/segment 频率 */
    private static final double RAM_BUFFER_SIZE_MB = 32D;

    private static final int MAX_MERGED_SEGMENT_MB = 128;

    private static final int SEGMENTS_PER_TIER = 4;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Object lifecycleLock = new Object();

    private final File indexPath;

    private final Directory directory;

    private final Analyzer analyzer;

    private IndexWriter indexWriter;

    private SearcherManager searcherManager;

    private volatile boolean dirty;

    public Shard(String path) {
        try {
            Path dir = Paths.get(path);
            indexPath = new File(dir.toUri());
            directory = FSDirectory.open(dir);
            analyzer = new SmartChineseAnalyzer();
            openWriterAndSearcher();
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

    public void updateBatch(List<Term> terms, List<Document> docs) {
        if (terms == null || docs == null || terms.isEmpty() || docs.size() != terms.size()) {
            return;
        }
        execute(docs, () -> {
            for (int i = 0; i < terms.size(); i++) {
                Term term = terms.get(i);
                if (term != null) {
                    indexWriter.updateDocument(term, docs.get(i));
                }
            }
        });
    }

    public void deleteBatch(Term... terms) {
        if (null != terms) {
            execute(terms, () -> indexWriter.deleteDocuments(terms));
        }
    }

    public void delete(Query query) {
        execute(query, () -> indexWriter.deleteDocuments(query));
    }

    public void deleteAll() {
        synchronized (lifecycleLock) {
            try {
                commitIfDirty();
                closeWriterAndSearcher();
                IOUtils.close(directory);
                FileUtils.deleteDirectory(indexPath);
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
    }

    public void close() throws IOException {
        synchronized (lifecycleLock) {
            commitIfDirty();
            closeWriterAndSearcher();
            IOUtils.close(analyzer, directory);
        }
    }

    /**
     * 存在未提交变更时执行 commit、清理废弃文件并刷新 Searcher。
     *
     * @return 是否实际执行了 commit
     */
    public boolean commitIfDirty() throws IOException {
        synchronized (lifecycleLock) {
            if (!dirty || !isWriterOpen()) {
                return false;
            }
            commitAndRefresh();
            dirty = false;
            return true;
        }
    }

    public Paging query(Option option, int pageNum, int pageSize, Sort sort) throws IOException {
        final IndexSearcher searcher;
        synchronized (lifecycleLock) {
            if (!isWriterOpen()) {
                openWriterAndSearcher();
            }
            // 读前提交，保证配置保存后立即可查
            commitIfDirty();
            searcherManager.maybeRefresh();
            searcher = searcherManager.acquire();
        }
        try {
            final TopDocs topDocs = getTopDocs(searcher, option.getQuery(), sort);
            Paging paging = new Paging(pageNum, pageSize);
            paging.setTotal(topDocs.totalHits.value);
            if (option.isQueryTotal()) {
                return paging;
            }
            List<Map> data = search(searcher, topDocs, option, pageNum, pageSize);
            paging.setData(data);
            return paging;
        } finally {
            synchronized (lifecycleLock) {
                if (searcherManager != null) {
                    searcherManager.release(searcher);
                }
            }
        }
    }

    private TopDocs getTopDocs(IndexSearcher searcher, Query query, Sort sort) throws IOException {
        if (null != sort) {
            return searcher.search(query, MAX_SIZE, sort);
        }
        return searcher.search(query, MAX_SIZE);
    }

    private List<Map> search(IndexSearcher searcher, TopDocs topDocs, Option option, int pageNum, int pageSize) throws IOException {
        ScoreDoc[] docs = topDocs.scoreDocs;
        int total = docs.length;
        int begin = (pageNum - 1) * pageSize;
        int end = pageNum * pageSize;

        begin = Math.min(begin, total);
        end = Math.min(end, total);

        List<Map> list = new ArrayList<>();
        Document doc;
        Map r;
        IndexableField f;
        Iterator<IndexableField> iterator;
        while (begin < end) {
            doc = searcher.doc(docs[begin++].doc);
            iterator = doc.iterator();
            r = new HashMap<>();
            while (iterator.hasNext()) {
                f = iterator.next();
                final String key = f.name();
                if (!option.includeField(key)) {
                    continue;
                }
                if (option.isEnableHighLightSearch()) {
                    try {
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
        synchronized (lifecycleLock) {
            if (!isWriterOpen()) {
                try {
                    openWriterAndSearcher();
                } catch (IOException e) {
                    logger.error("索引重开失败：{}", indexPath.getAbsolutePath(), e);
                    logger.error("索引异常数据：{}", value);
                    return;
                }
            }
            try {
                callback.execute();
                dirty = true;
            } catch (IOException e) {
                logger.error("索引异常：{}", indexPath.getAbsolutePath(), e);
                try {
                    closeWriterAndSearcher();
                } catch (IOException ex) {
                    logger.warn("关闭索引 Writer 失败：{}", indexPath.getAbsolutePath(), ex);
                }
            }
        }
    }

    private void commitAndRefresh() throws IOException {
        indexWriter.commit();
        indexWriter.deleteUnusedFiles();
        if (searcherManager != null) {
            searcherManager.maybeRefresh();
        }
    }

    private boolean isWriterOpen() {
        return indexWriter != null && indexWriter.isOpen();
    }

    private void openWriterAndSearcher() throws IOException {
        obtainWriteLockWithRetry();
        closeWriterAndSearcher();

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setRAMBufferSizeMB(RAM_BUFFER_SIZE_MB);
        config.setCommitOnClose(true);
        config.setMergePolicy(new TieredMergePolicy()
                .setMaxMergedSegmentMB(MAX_MERGED_SEGMENT_MB)
                .setSegmentsPerTier(SEGMENTS_PER_TIER)
                .setMaxMergeAtOnce(2));
        ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
        mergeScheduler.setMaxMergesAndThreads(2, 1);
        config.setMergeScheduler(mergeScheduler);

        indexWriter = new IndexWriter(directory, config);
        searcherManager = new SearcherManager(indexWriter, null);
        dirty = false;
    }

    private void closeWriterAndSearcher() throws IOException {
        IOUtils.close(searcherManager, indexWriter);
        searcherManager = null;
        indexWriter = null;
        dirty = false;
    }

    private void obtainWriteLockWithRetry() throws IOException {
        int maxRetries = 3;
        for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
            try {
                Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
                if (writeLock != null) {
                    IOUtils.close(writeLock);
                }
                return;
            } catch (LockObtainFailedException e) {
                logger.warn("无法获取 Lucene 写锁 (尝试 {}/{}), 尝试清理锁文件: {}", retryCount + 1, maxRetries, indexPath.getAbsolutePath());
                if (retryCount >= maxRetries - 1) {
                    throw new IOException("无法获取 Lucene 写锁，已重试 " + maxRetries + " 次。请检查是否有其他进程正在使用索引目录: " + indexPath.getAbsolutePath(), e);
                }
                clearStaleLockFile();
            }
        }
    }

    private void clearStaleLockFile() {
        try {
            File lockFile = new File(indexPath, IndexWriter.WRITE_LOCK_NAME);
            if (lockFile.exists()) {
                boolean deleted = lockFile.delete();
                if (deleted) {
                    logger.info("已删除残留的 Lucene 锁文件: {}", lockFile.getAbsolutePath());
                    sleepQuietly(100);
                } else {
                    logger.warn("无法删除 Lucene 锁文件，可能被其他进程占用: {}", lockFile.getAbsolutePath());
                    sleepQuietly(500);
                }
            } else {
                sleepQuietly(200);
            }
        } catch (Exception ex) {
            logger.error("清理 Lucene 锁文件失败: {}", indexPath.getAbsolutePath(), ex);
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    interface Callback {

        void execute() throws IOException;
    }
}
