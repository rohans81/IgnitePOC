package poc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;

/**
 * Created by rohan.shetty on 29/05/2017.
 */
public class CacheTest {

    public static void main (String[] args) throws Exception {
        IgniteConfig cacheTest = new IgniteConfig();
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
}
