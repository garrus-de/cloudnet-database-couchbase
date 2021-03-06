package de.garrus.cloudnet.couchbase;

import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.google.common.base.Preconditions;
import de.dytanic.cloudnet.common.collection.NetorHashMap;
import de.dytanic.cloudnet.common.collection.Pair;
import de.dytanic.cloudnet.common.document.gson.JsonDocument;
import de.dytanic.cloudnet.database.AbstractDatabaseProvider;
import de.dytanic.cloudnet.driver.database.Database;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Getter
public class CouchbaseDatabaseProvider extends AbstractDatabaseProvider {
    private static final long NEW_CREATION_DELAY = 600000L;
    protected final NetorHashMap<String, Long, Database> cachedDatabaseInstances = new NetorHashMap<>();
    private final JsonDocument config;
    private final boolean autoShutdownExecutorService;
    private Cluster cluster;
    private Bucket bucket;

    private final String scope;
    private ExecutorService service;

    public CouchbaseDatabaseProvider(JsonDocument config, ExecutorService executorService) {
        this.config = config;
        this.scope = config.getString("scope");
        this.service = executorService;
        if (executorService != null) {
            this.service = executorService;
        } else {
            this.service = Executors.newCachedThreadPool();
        }
        this.autoShutdownExecutorService = executorService == null;
    }

    public boolean init() {

        ClusterEnvironment environment = ClusterEnvironment.builder().loggerConfig(LoggerConfig.customLogger(new CouchbaseCloudNetLogger())).build();
        ClusterOptions options = ClusterOptions.clusterOptions(config.getString("username"), config.getString("password")).environment(environment);

        this.cluster = Cluster.connect(config.getString("host"), options);
        this.bucket = cluster.bucket(config.getString("bucket"));
        return true;
    }

    public Database getDatabase(String name) {
        Preconditions.checkNotNull(name);
        this.removedOutdatedEntries();
        if (!this.cachedDatabaseInstances.contains(name)) {
            this.cachedDatabaseInstances.add(name, System.currentTimeMillis() + NEW_CREATION_DELAY, new CouchbaseDatabase(this, name, service) {
            });
        }

        return this.cachedDatabaseInstances.getSecond(name);
    }

    public boolean containsDatabase(String name) {
        Preconditions.checkNotNull(name);
        this.removedOutdatedEntries();

        return getDatabaseNames().contains(name);
    }

    public boolean deleteDatabase(String name) {
        bucket.collections().dropCollection(CollectionSpec.create(name, bucket.defaultScope().name()));
        return true;
    }

    public List<String> getDatabaseNames() {
        List<String> names = new ArrayList<>();
        for (ScopeSpec allScope : bucket.collections().getAllScopes()) {
            for (CollectionSpec collection : allScope.collections()) {
                names.add(collection.name());
            }
        }
        return names;
    }

    public String getName() {
        return this.config.getString("database");
    }

    public void close() {
        if (autoShutdownExecutorService) {
            this.service.shutdownNow();
        }
        this.cluster.disconnect();
    }

    private void removedOutdatedEntries() {
        Iterator<Map.Entry<String, Pair<Long, Database>>> var1 = this.cachedDatabaseInstances.entrySet().iterator();

        while (var1.hasNext()) {
            Map.Entry<String, Pair<Long, Database>> entry = var1.next();
            if ((Long) ((Pair) entry.getValue()).getFirst() < System.currentTimeMillis()) {
                this.cachedDatabaseInstances.remove(entry.getKey());
            }
        }

    }

    public NetorHashMap<String, Long, Database> getCachedDatabaseInstances() {
        return this.cachedDatabaseInstances;
    }

}