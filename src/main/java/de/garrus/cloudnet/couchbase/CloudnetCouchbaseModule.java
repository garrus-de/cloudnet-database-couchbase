package de.garrus.cloudnet.couchbase;

import de.dytanic.cloudnet.database.AbstractDatabaseProvider;
import de.dytanic.cloudnet.driver.module.ModuleLifeCycle;
import de.dytanic.cloudnet.driver.module.ModuleTask;
import de.dytanic.cloudnet.module.NodeCloudNetModule;

public class CloudnetCouchbaseModule extends NodeCloudNetModule {

    private static CloudnetCouchbaseModule instance;

    public static CloudnetCouchbaseModule getInstance() {
        return instance;
    }

    @ModuleTask(order = 127, event = ModuleLifeCycle.LOADED)
    public void init() {
        instance = this;
    }

    @ModuleTask(order = 126, event = ModuleLifeCycle.LOADED)
    public void initConfig() {
        this.getConfig().getString("host", "localhost");
        this.getConfig().getString("username", "root");
        this.getConfig().getString("password", "root");
        this.getConfig().getString("bucket", "cloudnet-meta");
        this.saveConfig();
    }

    @ModuleTask(order = 125, event = ModuleLifeCycle.LOADED)
    public void registerDatabaseProvider() {
        this.getRegistry().registerService(AbstractDatabaseProvider.class, "couchbase", new CouchbaseDatabaseProvider(getConfig(),null));
    }

    @ModuleTask(order = 127, event = ModuleLifeCycle.STOPPED)
    public void unregisterDatabaseProvider() {
        this.getRegistry().unregisterService(AbstractDatabaseProvider.class, "couchbase");
    }
}
