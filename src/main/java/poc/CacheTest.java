package poc;

import com.datastax.driver.core.policies.LoadBalancingPolicy;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.store.cassandra.CassandraCacheStoreFactory;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.context.annotation.Bean;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by rohan.shetty on 29/05/2017.
 */
public class CacheTest {

    private Boolean peerClassLoadingEnabled = false;

    private String gridName = "API";

    private Boolean clientMode = true;

    private Long metricsLogFrequency = 0l;

    private Boolean binaryConfigurationCompactFooter = false;

    private String discoveryAddresses= "127.0.0.1";



    //CACHE properties
    private CacheMode cacheMode = CacheMode.PARTITIONED;

    private int cacheBackups = 0;

    private Boolean cacheStatisticsEnabled = false;

    private Boolean useCassandra = true;

    private Boolean cacheReadThroughEnabled= true;

    private Boolean cacheWriteThroughEnabled = true;

    private Boolean cacheWriteBehindEnabled = true;

    private Optional<Integer> maxSizeBeforeLruEviction = Optional.ofNullable(null);

    private Long writeBehindFlushFrequency=10000l;

    private Boolean storeKeepBinaryEnabled = true;

    private String[] contactPoints= {"127.0.0.1"};

    private String readConsistency="ONE";

    private String writeConsistency="ONE";

    //    @Value("${ignite.cassandra.load.balancing.policy.class:#{null}}")
    private String loadBalancingPolicyClass=null;


    private String cacheStoreKeyClass= "java.lang.String";

    private String cacheStoreKeyStrategy= "PRIMITIVE";

    private String cacheStoreValueClass="org.apache.ignite.binary.BinaryObject";

    private String cacheStoreValueStrategy="BLOB";

    private boolean useExpiryPolicy=false;

    private Long expiryPolicyMinutes=15l;

    private static final String CASSANDRA_PERSISTENCE = "<persistence keyspace=\"%s\" table=\"%s\">" +
            "<keyPersistence class=\"%s\" strategy=\"%s\"/>" +
            "<valuePersistence class=\"%s\" strategy=\"%s\"/>" +
            "</persistence>";

    private Ignite ignite;

    public static void main (String[] args) throws Exception {
        CacheTest cacheTest = new CacheTest();
        SegmentBinaryMapper mapper = new SegmentBinaryMapper();
        Ignite ignite = (Ignition.getOrStart(cacheTest.igniteConfiguration()));

        IgniteCache<String,BinaryObject> cache = ignite.getOrCreateCache(cacheTest.getAllCacheConfigurations()[0].setStoreKeepBinary(true)).withKeepBinary();
        Segment segment = new Segment("id1","select * from test", "client1");
       cache.put(segment.getId(),mapper.toBinaryObject(ignite.binary(),segment)); //PUT ONLY IN FIRST RUN
        System.out.println(mapper.fromBinaryObject(cache.get(segment.getId()))); //PASSING
        cache.loadCache(null); //FAILING
        cache.loadCache(null, "select * from test.segment"); //FAILING
        System.out.printf("");
    }


    /**
     * @return the {@link IgniteConfiguration} configured
     */
    @Bean
    public IgniteConfiguration igniteConfiguration() throws ClassNotFoundException {
        final IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setGridName(gridName);
        igniteConfiguration.setClientMode(clientMode);
        igniteConfiguration.setGridLogger(new Slf4jLogger());
        igniteConfiguration.setMetricsLogFrequency(metricsLogFrequency);
        igniteConfiguration.setCacheConfiguration(getAllCacheConfigurations());
        igniteConfiguration.setDiscoverySpi(tcpDiscoverySpi());
        // Make false to get around https://issues.apache.org/jira/browse/IGNITE-2779
        final BinaryConfiguration binaryConfiguration = new BinaryConfiguration();
        binaryConfiguration.setCompactFooter(binaryConfigurationCompactFooter);
        igniteConfiguration.setBinaryConfiguration(binaryConfiguration);
        igniteConfiguration.setPeerClassLoadingEnabled(peerClassLoadingEnabled);
        return igniteConfiguration;
    }

    /**
     * @return the {@link TcpDiscoverySpi} configured
     */
    @Bean
    public TcpDiscoverySpi tcpDiscoverySpi() {
        final TcpDiscoveryVmIpFinder tcpDiscoveryVmIpFinder = new TcpDiscoveryVmIpFinder();
        tcpDiscoveryVmIpFinder.setAddresses(new ArrayList<>(Arrays.asList(discoveryAddresses.split(","))));
        final TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        tcpDiscoverySpi.setIpFinder(tcpDiscoveryVmIpFinder);
        return tcpDiscoverySpi;
    }


    public CacheConfiguration<String, BinaryObject>[] getAllCacheConfigurations() throws ClassNotFoundException {
        String modelClass= "Segment";
        String cacheName ="segment";
        String cassandraTable= "segment";
        String cassandraKeyspace="test";

        CacheConfiguration cacheConfiguration = getCacheConfiguration(new Config(modelClass,cacheName,cassandraTable, cassandraKeyspace));
        return new CacheConfiguration[] {cacheConfiguration};
    }


    public CacheConfiguration<String, BinaryObject> getCacheConfiguration(Config cacheConfig) throws ClassNotFoundException {
        final CacheConfiguration <String, BinaryObject> cacheConfiguration = getCommonCacheConfiguration();
        cacheConfiguration.setName(cacheConfig.getCacheName());
        final CassandraCacheStoreFactory<String, BinaryObject> cacheStoreFactory = new CassandraCacheStoreFactory<>();
        if (useCassandra){
            cacheStoreFactory.setDataSource(getDataSource());
            final KeyValuePersistenceSettings settings = new KeyValuePersistenceSettings(
                    String.format(CASSANDRA_PERSISTENCE, cacheConfig.getCassandraKeyspace(), cacheConfig.getCassandraTable(),
                            cacheStoreKeyClass, cacheStoreKeyStrategy, cacheStoreValueClass, cacheStoreValueStrategy));
            cacheStoreFactory.setPersistenceSettings(settings);
            cacheConfiguration.setCacheStoreFactory(cacheStoreFactory);
        }
        // cacheConfiguration.setQueryEntities(getQueryEntities(cacheConfig));
        return cacheConfiguration;
    }

    private List<QueryEntity> getQueryEntities(Config cacheConfig) throws ClassNotFoundException {
        final QueryEntity queryEntity = new QueryEntity();
        queryEntity.setKeyType("java.lang.String");
        queryEntity.setValueType(cacheConfig.getCacheName());
        queryEntity.setFields(cacheConfig.getFieldMapping());
        return Collections.singletonList(queryEntity);
    }
    public  CacheConfiguration <String, BinaryObject> getCommonCacheConfiguration () {
        final CacheConfiguration<String, BinaryObject> cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setStoreKeepBinary(storeKeepBinaryEnabled);
        cacheConfiguration.setCacheMode(cacheMode);
        cacheConfiguration.setBackups(cacheBackups);
        cacheConfiguration.setStatisticsEnabled(cacheStatisticsEnabled);

        if (useExpiryPolicy) {
            cacheConfiguration.setExpiryPolicyFactory(
                    CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, expiryPolicyMinutes)));
        }

        if (useCassandra) {
            cacheConfiguration.setReadThrough(cacheReadThroughEnabled);
            cacheConfiguration.setWriteThrough(cacheWriteThroughEnabled);
            cacheConfiguration.setWriteBehindEnabled(cacheWriteBehindEnabled);
            cacheConfiguration.setWriteBehindFlushFrequency(writeBehindFlushFrequency);
            maxSizeBeforeLruEviction.ifPresent(
                    maxCacheLifetime -> cacheConfiguration.setEvictionPolicy(new LruEvictionPolicy(maxCacheLifetime)));
        }
        return cacheConfiguration;
    }

    private DataSource getDataSource () {
        final DataSource dataSource = new DataSource();
        dataSource.setContactPoints(contactPoints);
        dataSource.setReadConsistency(readConsistency);
        dataSource.setWriteConsistency(writeConsistency);
        if (!StringUtils.isEmpty(loadBalancingPolicyClass)) {
            try {
                final LoadBalancingPolicy loadBalancingPolicy = (LoadBalancingPolicy) Class.forName(loadBalancingPolicyClass).newInstance();
                if (!(loadBalancingPolicy instanceof Serializable)) {
                    final IllegalArgumentException iae = new IllegalArgumentException("LoadingBalancingPolicy needs to implement Serializable");
                    throw iae;
                }
                dataSource.setLoadBalancingPolicy(loadBalancingPolicy);
            } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return dataSource;
    }

    @Data
    class IgniteCassandra {

        List<Config> configs = new ArrayList<Config>();

    }
    @Data
    @AllArgsConstructor
    class Config {
        private String modelClass;
        private String cacheName;
        private String cassandraTable;
        private String cassandraKeyspace;

        public LinkedHashMap<String, String> getFieldMapping () throws ClassNotFoundException {
            LinkedHashMap <String,String> map = new LinkedHashMap<>();
            Arrays.stream(Class.forName(modelClass).getDeclaredFields()).forEach(field -> map.put(field.getName(), field.getType().getName()));
            return map;
        }
    }


}


