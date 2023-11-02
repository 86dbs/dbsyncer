package org.dbsyncer.connector.es;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptySet;

public class EasyRestHighLevelClient extends RestHighLevelClient {

    private Version version;

    public EasyRestHighLevelClient(RestClientBuilder restClientBuilder) {
        super(restClientBuilder);
    }

    protected EasyRestHighLevelClient(RestClientBuilder restClientBuilder, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        super(restClientBuilder, namedXContentEntries);
    }

    protected EasyRestHighLevelClient(RestClient restClient, CheckedConsumer<RestClient, IOException> doClose, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        super(restClient, doClose, namedXContentEntries);
    }

    /**
     * Executes a bulk request using the Bulk API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">Bulk API on elastic.co</a>
     * @param bulkRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final BulkResponse bulkWithVersion(BulkRequest bulkRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(bulkRequest, RequestConverters::bulk, options, BulkResponse::fromXContent, emptySet());
    }

    /**
     * Executes a search request using the Search API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html">Search API on elastic.co</a>
     *
     * @param searchRequest the request
     * @param options       the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final SearchResponse searchWithVersion(SearchRequest searchRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(searchRequest, r -> RequestConverters.search(r, "_search", version), options, SearchResponse::fromXContent, Collections.emptySet());
    }

    /**
     * Executes a request using the Search Template API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-template.html">Search Template API
     * on elastic.co</a>.
     *
     * @param searchTemplateRequest the request
     * @param options               the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final SearchTemplateResponse searchTemplateWithVersion(SearchTemplateRequest searchTemplateRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(searchTemplateRequest, r -> RequestConverters.searchTemplate(r, version), options, SearchTemplateResponse::fromXContent, emptySet());
    }


    /**
     * Asynchronously executes a request using the Search Template API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-template.html">Search Template API
     * on elastic.co</a>.
     *
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable searchTemplateAsyncWithVersion(SearchTemplateRequest searchTemplateRequest, RequestOptions options, ActionListener<SearchTemplateResponse> listener) {
        return performRequestAsyncAndParseEntity(searchTemplateRequest, r -> RequestConverters.searchTemplate(r, version), options,
                SearchTemplateResponse::fromXContent, listener, emptySet());
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }
}
