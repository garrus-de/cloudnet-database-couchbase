package de.garrus.cloudnet.couchbase;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryResult;
import de.dytanic.cloudnet.CloudNet;
import de.dytanic.cloudnet.common.concurrent.ITask;
import de.dytanic.cloudnet.common.concurrent.ListenableTask;
import de.dytanic.cloudnet.common.document.gson.JsonDocument;
import de.dytanic.cloudnet.driver.database.Database;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

public class CouchbaseDatabase implements Database {
    private final CouchbaseDatabaseProvider provider;
    private final String name;
    private final Collection collection;
    private final ExecutorService executorService;

    public CouchbaseDatabase(CouchbaseDatabaseProvider couchbaseDatabaseProvider, String name, ExecutorService service) {
        this.provider = couchbaseDatabaseProvider;
        boolean exists = false;
        for (ScopeSpec allScope : provider.getBucket().collections().getAllScopes()) {
            for (CollectionSpec collection : allScope.collections()) {
                if (collection.name().equals(name)) {
                    exists = true;
                }
            }
        }

        if (!exists) {
            provider.getBucket().collections().createCollection(CollectionSpec.create(name, provider.getBucket().defaultScope().name()));
        }

        this.collection = provider.getBucket().collection(name);

        if (!exists) {
            provider.getCluster().query("CREATE PRIMARY INDEX ON `default`:`" + collection.bucketName() + "`.`" + collection.scopeName() + "`.`" + collection.name() + "`");
        }
        this.name = name;
        this.executorService = service;
    }

    @Override
    public boolean insert(String key, JsonDocument document) {
        collection.insert(key, JsonObject.fromJson(document.toJson()));
        return true;
    }

    @Override
    public boolean update(String key, JsonDocument document) {
        collection.upsert(key, JsonObject.fromJson(document.toJson()));
        return true;
    }

    @Override
    public boolean contains(String key) {
        return collection.exists(key).exists();
    }

    @Override
    public boolean delete(String key) {
        collection.remove(key);
        return true;
    }

    @Override
    public JsonDocument get(String key) {
        try {
            return JsonDocument.newDocument(collection.get(key).contentAsObject().toString());
        } catch (DocumentNotFoundException e) {
            return null;
        }
    }

    @Override
    public List<JsonDocument> get(String fieldName, Object fieldValue) {
        QueryResult query = provider.getCluster().query("SELECT * FROM `" + collection.bucketName() + "`." + collection.scopeName() + "." + collection.name() + " as data WHERE " + fieldName + " =\"" + fieldValue.toString() + "\"");
        List<JsonDocument> documents = new ArrayList<>();

        for (JsonObject jsonObject : query.rowsAsObject()) {
            CloudNet.getInstance().getLogger().info(jsonObject.toString());
            documents.add(JsonDocument.newDocument(jsonObject.getObject("data").toString()));
        }

        return documents;
    }

    @Override
    public List<JsonDocument> get(JsonDocument filters) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM `" + collection.bucketName() + "`." + collection.scopeName() + "." + collection.name());

        if (filters.isEmpty()) {
            sql.append(" WHERE ");
            boolean first = true;
            for (String key : filters.keys()) {
                if (!first) {
                    sql.append(" AND ");
                } else {
                    first = false;
                }
                sql.append(key);
                sql.append(" = \"");
                sql.append(filters.getString(key));
                sql.append("\"");
            }
        }

        QueryResult query = provider.getCluster().query(sql.toString());

        List<JsonDocument> documents = new ArrayList<>();

        for (JsonObject jsonObject : query.rowsAsObject()) {
            documents.add(JsonDocument.newDocument(jsonObject.getObject("data")));
        }

        return documents;
    }

    @Override
    public java.util.Collection<String> keys() {
        QueryResult query = provider.getCluster().query("SELECT meta().id FROM `" + collection.bucketName() + "`." + collection.scopeName() + "." + collection.name());
        List<String> keys = new ArrayList<>();
        for (JsonObject jsonObject : query.rowsAsObject()) {
            keys.add(jsonObject.getString("id"));
        }
        return keys;
    }

    @Override
    public java.util.Collection<JsonDocument> documents() {
        QueryResult query = provider.getCluster().query("SELECT * FROM `" + collection.bucketName() + "`." + collection.scopeName() + "." + collection.name() + " as data");
        List<JsonDocument> documents = new ArrayList<>();
        for (JsonObject jsonObject : query.rowsAsObject()) {
            documents.add(JsonDocument.newDocument(jsonObject));
        }

        return documents;
    }

    @Override
    public Map<String, JsonDocument> entries() {
        QueryResult query = provider.getCluster().query("SELECT meta().id,* FROM `" + collection.bucketName() + "`." + collection.scopeName() + "." + collection.name() + " as data");
        Map<String, JsonDocument> entries = new HashMap<>();
        for (JsonObject jsonObject : query.rowsAsObject()) {
            entries.put(jsonObject.getString("id"), JsonDocument.newDocument(jsonObject.getObject("data")));
        }
        return entries;
    }

    @Override
    public Map<String, JsonDocument> filter(BiPredicate<String, JsonDocument> predicate) {
        Map<String, JsonDocument> filtered = new HashMap<>();
        entries().forEach((key, document) -> {
            if (predicate.test(key, document)) {
                filtered.put(key, document);
            }
        });
        return filtered;
    }

    @Override
    public void iterate(BiConsumer<String, JsonDocument> consumer) {
        entries().forEach(consumer);
    }

    @Override
    public void clear() {
        for (String key : keys()) {
            collection.remove(key);
        }
    }

    @Override
    public long getDocumentsCount() {
        return keys().size();
    }

    @Override
    public boolean isSynced() {
        return false;
    }

    @Override
    public @NotNull ITask<Boolean> insertAsync(String key, JsonDocument document) {
        return schedule(() -> this.insert(key, document));
    }

    @Override
    public @NotNull ITask<Boolean> updateAsync(String key, JsonDocument document) {
        return schedule(() -> this.update(key, document));
    }

    @Override
    public @NotNull ITask<Boolean> containsAsync(String key) {
        return schedule(() -> this.contains(key));
    }

    @Override
    public @NotNull ITask<Boolean> deleteAsync(String key) {
        return schedule(() -> this.delete(key));
    }

    @Override
    public @NotNull ITask<JsonDocument> getAsync(String key) {
        return schedule(() -> this.get(key));
    }

    @Override
    public @NotNull ITask<List<JsonDocument>> getAsync(String fieldName, Object fieldValue) {
        return schedule(() -> this.get(fieldName, fieldValue));
    }

    @Override
    public @NotNull ITask<List<JsonDocument>> getAsync(JsonDocument filters) {
        return schedule(() -> this.get(filters));
    }

    @Override
    public @NotNull ITask<java.util.Collection<String>> keysAsync() {
        return schedule(this::keys);
    }

    @Override
    public @NotNull ITask<java.util.Collection<JsonDocument>> documentsAsync() {
        return schedule(this::documents);
    }

    @Override
    public @NotNull ITask<Map<String, JsonDocument>> entriesAsync() {
        return schedule(this::entries);
    }

    @Override
    public @NotNull ITask<Map<String, JsonDocument>> filterAsync(BiPredicate<String, JsonDocument> predicate) {
        return schedule(() -> this.filter(predicate));
    }

    @Override
    public @NotNull ITask<Void> iterateAsync(BiConsumer<String, JsonDocument> consumer) {
        return this.schedule(() -> {
            this.iterate(consumer);
            return null;
        });
    }

    @Override
    public @NotNull ITask<Void> clearAsync() {
        return this.schedule(() -> {
            this.clear();
            return null;
        });
    }

    @Override
    public @NotNull ITask<Long> getDocumentsCountAsync() {
        return schedule(this::getDocumentsCount);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws Exception {
    }

    @NotNull
    private <T> ITask<T> schedule(Callable<T> callable) {
        ITask<T> task = new ListenableTask<>(callable);
        this.executorService.execute(() -> {
            try {
                Thread.sleep(0, 100000);
                task.call();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        });
        return task;
    }
}
