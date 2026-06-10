/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
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
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.IOUtils;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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

    /** searchAfter 跳页时每批跳过的文档数，控制单次堆内存占用 */
    private static final int SEARCH_AFTER_STEP = 500;

    private static final double RAM_BUFFER_SIZE_MB = 16D;

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
            analyzer = new SimpleAnalyzer();
//            analyzer = new SmartChineseAnalyzer();
            openWriterAndSearcher();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public void insertBatch(List<Document> docs) {
        if (CollectionUtils.isEmpty(docs)) {
            return;
        }
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
        if (terms != null && terms.length > 0) {
            execute(terms, () -> indexWriter.deleteDocuments(terms));
        }
    }

    public void delete(Query query) {
        if (query != null) {
            execute(query, () -> indexWriter.deleteDocuments(query));
        }
    }

    public void deleteAll() {
        synchronized (lifecycleLock) {
            try {
                commitIfDirty();
                closeWriterAndSearcher();
                IOUtils.close(directory, analyzer);
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

    public boolean isDirty() {
        return dirty;
    }

    public Paging query(Option option, int pageNum, int pageSize, Sort sort) throws IOException {
        if (option == null || option.getQuery() == null) {
            return new Paging(pageNum, pageSize);
        }
        if (pageNum < 1) {
            pageNum = 1;
        }
        if (pageSize < 1) {
            pageSize = 20;
        }

        final IndexSearcher searcher;
        synchronized (lifecycleLock) {
            ensureWriterOpen();
            prepareSearcherForRead();
            searcher = searcherManager.acquire();
        }
        try {
            Paging paging = new Paging(pageNum, pageSize);
            Query query = option.getQuery();

            long total = searcher.count(query);
            paging.setTotal(total);

            if (option.isQueryTotal()) {
                return paging;
            }

            int offset = (pageNum - 1) * pageSize;
            if (offset >= total) {
                paging.setData(new ArrayList<>());
                return paging;
            }

            Sort pagingSort = ensureSortForPaging(sort);
            ScoreDoc after = skipToOffset(searcher, query, pagingSort, offset);
            TopDocs pageDocs = searchAfter(searcher, after, query, pageSize, pagingSort);
            paging.setData(search(searcher, pageDocs, option));
            return paging;
        } finally {
            searcherManager.release(searcher);
        }
    }

    /**
     * 读前：有脏数据则 commit 并阻塞刷新；否则轻量 maybeRefresh。
     */
    private void prepareSearcherForRead() throws IOException {
        if (dirty && isWriterOpen()) {
            commitAndRefresh();
            dirty = false;
        } else if (searcherManager != null) {
            searcherManager.maybeRefresh();
        }
    }

    /**
     * searchAfter 要求排序稳定，追加 _doc 作为 tie-breaker。
     */
    private Sort ensureSortForPaging(Sort sort) {
        if (sort == null) {
            return null;
        }
        for (SortField field : sort.getSort()) {
            if (SortField.FIELD_DOC.equals(field)) {
                return sort;
            }
        }
        SortField[] fields = sort.getSort();
        SortField[] extended = Arrays.copyOf(fields, fields.length + 1);
        extended[fields.length] = SortField.FIELD_DOC;
        return new Sort(extended);
    }

    /**
     * 使用 searchAfter 分批跳过 offset，避免深分页一次性加载大量 ScoreDoc。
     */
    private ScoreDoc skipToOffset(IndexSearcher searcher, Query query, Sort sort, int offset) throws IOException {
        if (offset <= 0) {
            return null;
        }
        ScoreDoc after = null;
        int skipped = 0;
        while (skipped < offset) {
            int fetch = Math.min(SEARCH_AFTER_STEP, offset - skipped);
            TopDocs page = searchAfter(searcher, after, query, fetch, sort);
            ScoreDoc[] hits = page.scoreDocs;
            if (hits.length == 0) {
                break;
            }
            after = hits[hits.length - 1];
            skipped += hits.length;
            if (hits.length < fetch) {
                break;
            }
        }
        return after;
    }

    private TopDocs searchAfter(IndexSearcher searcher, ScoreDoc after, Query query, int numHits, Sort sort) throws IOException {
        if (sort != null) {
            return searcher.searchAfter(after, query, numHits, sort);
        }
        return searcher.searchAfter(after, query, numHits);
    }

    private List<Map> search(IndexSearcher searcher, TopDocs topDocs, Option option) throws IOException {
        ScoreDoc[] docs = topDocs.scoreDocs;
        List<Map> list = new ArrayList<>(docs.length);
        for (ScoreDoc scoreDoc : docs) {
            Document doc = searcher.doc(scoreDoc.doc);
            Map<String, Object> row = new HashMap<>();
            Iterator<IndexableField> iterator = doc.iterator();
            while (iterator.hasNext()) {
                IndexableField field = iterator.next();
                final String key = field.name();
                if (!option.includeField(key)) {
                    continue;
                }
                if (option.isEnableHighLightSearch() && option.getHighLightKeys().contains(key)) {
                    row.put(key, highlightField(doc, key, option));
                    continue;
                }
                row.put(key, option.getFieldResolver(key).getValue(field));
            }
            list.add(row);
        }
        return list;
    }

    private String highlightField(Document doc, String key, Option option) {
        String content = doc.get(key);
        if (content == null) {
            return null;
        }
        try (TokenStream tokenStream = analyzer.tokenStream(key, content)) {
            return option.getHighlighter().getBestFragment(tokenStream, content);
        } catch (IOException | InvalidTokenOffsetsException e) {
            logger.error("高亮解析失败, field={}", key, e);
            return content;
        }
    }

    private void execute(Object value, Callback callback) {
        if (value == null) {
            return;
        }
        synchronized (lifecycleLock) {
            try {
                ensureWriterOpen();
                callback.execute();
                dirty = true;
            } catch (IOException e) {
                logger.error("索引异常：{}", indexPath.getAbsolutePath(), e);
                closeWriterQuietly();
            }
        }
    }

    private void commitAndRefresh() throws IOException {
        indexWriter.commit();
        indexWriter.deleteUnusedFiles();
        if (searcherManager != null) {
            // 阻塞刷新，保证 commit 后紧接着的查询可见
            searcherManager.maybeRefreshBlocking();
        }
    }

    private void ensureWriterOpen() throws IOException {
        if (!isWriterOpen()) {
            openWriterAndSearcher();
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
        config.setUseCompoundFile(true);
        config.setMergePolicy(new TieredMergePolicy()
                .setMaxMergedSegmentMB(MAX_MERGED_SEGMENT_MB)
                .setSegmentsPerTier(SEGMENTS_PER_TIER)
                .setMaxMergeAtOnce(2)
                .setDeletesPctAllowed(20));
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

    private void closeWriterQuietly() {
        try {
            closeWriterAndSearcher();
        } catch (IOException e) {
            logger.warn("关闭索引 Writer 失败：{}", indexPath.getAbsolutePath(), e);
        }
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
