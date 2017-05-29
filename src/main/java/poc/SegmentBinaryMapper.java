package poc;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * {@link Service} for transforming {@link Segment} to {@link BinaryObject} for saving data in the ignite cache
 * and cassandra store
 */
@Service
public class SegmentBinaryMapper {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentBinaryMapper.class);

    public Segment fromBinaryObject(final BinaryObject binaryObject) throws IllegalArgumentException {
        if (binaryObject == null) {
            LOG.warn("binaryObject is null returning null");
            throw new IllegalArgumentException("binaryObject is null");
        }
        // If binaryObject doesn't exist the field - method field(name) will return null
        return new Segment(binaryObject.field("id"), binaryObject.field("query"), binaryObject.field("clientId"));
    }

    public BinaryObject toBinaryObject(final IgniteBinary igniteBinary, final Segment segment) {
        final BinaryObjectBuilder builder = igniteBinary.builder(Segment.class.getName());
        builder.setField("id", segment.getId());
        builder.setField("query", segment.getQuery());
        builder.setField("clientId", segment.getClientId());
        return builder.build();
    }
}
