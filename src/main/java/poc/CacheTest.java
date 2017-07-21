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

        Ignite ignite = Ignition.start("ignite_client.xml");

        IgniteCache<String,BinaryObject> cache = ignite.getOrCreateCache("DATA_CACHE");

        Segment segment = new Segment("id1","query1", "client1");
        SegmentBinaryMapper mapper = new SegmentBinaryMapper();
        BinaryObject binaryObject = mapper.toBinaryObject(ignite.binary(),segment);

        cache.put(segment.getId(),binaryObject); //PUT ONLY IN FIRST RUN

        System.out.println(mapper.fromBinaryObject(cache.get(segment.getId()))); //PASSING

        cache.loadCache(null); //FAILING

        System.out.printf("");
    }
}
