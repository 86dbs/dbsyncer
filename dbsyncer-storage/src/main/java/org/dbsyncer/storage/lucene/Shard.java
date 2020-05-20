package org.dbsyncer.storage.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.dbsyncer.storage.StorageException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/12 20:29
 */
public class Shard {

    private Directory directory;

    private Analyzer analyzer;

    private IndexWriter indexWriter;

    private IndexReader indexReader;

    private IndexWriterConfig config;

    private String path;

    private final Object lock = new Object();

    private static final int MAX_SIZE = 10000;

    public Shard(String path) throws IOException {
        this.path = path;
        // 索引存放的位置，设置在当前目录中
        directory = FSDirectory.open(Paths.get(path));
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
    }

    public List<Map> query(Query query, int pageNum, int pageSize) throws IOException {
        return executeQuery(query, pageNum, pageSize);
    }

    public void insert(Document doc) throws IOException {
        if (null != doc) {
            indexWriter.addDocument(doc);
            indexWriter.commit();
        }
    }

    public void insertBatch(List<Document> docs) throws IOException {
        if (null != docs) {
            indexWriter.addDocuments(docs);
            indexWriter.commit();
        }
    }

    public void update(Term term, Document doc) throws IOException {
        if (null != term && null != doc) {
            indexWriter.updateDocument(term, doc);
            indexWriter.commit();
        }
    }

    public void delete(Term term) throws IOException {
        if (null != term) {
            indexWriter.deleteDocuments(term);
            forceFlush();
        }
    }

    public void deleteAll() throws IOException {
        indexWriter.deleteAll();
        forceFlush();
    }

    public void close() {
        try {
            indexWriter.close();
            indexReader.close();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    private void forceFlush() throws IOException {
        indexWriter.commit();
        indexWriter.close();
    }

    private IndexSearcher getSearcher() throws IOException {
        // 复用索引读取器
        IndexReader changeReader = DirectoryReader.openIfChanged((DirectoryReader) indexReader, indexWriter, true);
        if (null != changeReader) {
            indexReader.close();
            indexReader = null;
            synchronized (lock) {
                if (null == indexReader) {
                    indexReader = changeReader;
                }
            }
        }
        return new IndexSearcher(indexReader);
    }

    /**
     * 执行查询，并打印查询到的记录数
     *
     * @param query
     * @param pageNum
     * @param pageSize
     * @throws IOException
     */
    private List<Map> executeQuery(Query query, int pageNum, int pageSize) throws IOException {
        IndexSearcher searcher = getSearcher();
        TopDocs topDocs = searcher.search(query, MAX_SIZE);

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
        while (begin < end) {
            //取得对应的文档对象
            doc = searcher.doc(docs[begin].doc);
            r = new LinkedHashMap<>();
            Iterator<IndexableField> iterator = doc.iterator();
            while (iterator.hasNext()) {
                f = iterator.next();
                r.put(f.name(), f.stringValue());
            }
            list.add(r);
            begin++;
        }
        return list;
    }

}