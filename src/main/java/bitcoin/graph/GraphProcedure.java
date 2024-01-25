package bitcoin.graph;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.procedure.*;
import utility.PathExpanderFactory;
import utility.SafeConvert;
import java.util.*;
import java.util.logging.Logger;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public class GraphProcedure {
    static final Label ACCOUNT = Label.label("account");

    static final Label TRANSACTION = Label.label("transaction");

    static final RelationshipType INPUT = RelationshipType.withName("input");

    static final RelationshipType OUTPUT = RelationshipType.withName("output");

    static final int EXCHANGE_THRESHOLD = 5000;

    static final int MAX_SEARCH_TIME = 10000;

    static final int DAY_RANGE = 86400;

    static final Logger logger = Logger.getLogger(GraphProcedure.class.getName());

    @Context
    public Transaction tx;

    private Iterator<Relationship> inbound(
        long maxRelationshipCount, 
        long minTimestamp, 
        long maxTimestamp, 
        long minValue, 
        long maxValue, 
        Node lastNode,
        long timeout
    ) {
        List<Relationship> relationships = new ArrayList<>();

        try {
            if (System.currentTimeMillis() > timeout) return relationships.iterator();

            if (lastNode.hasLabel(ACCOUNT)) {
                if (lastNode.getDegree() < EXCHANGE_THRESHOLD) {
                    lastNode.getRelationships(INCOMING, OUTPUT).stream().parallel()
                        .filter(output -> {
                            Node transaction = output.getOtherNode(lastNode);
                            long timestamp = SafeConvert.toLong(transaction.getProperty("timestamp", null), 0L);
                            long value = SafeConvert.toLong(transaction.getProperty("input_value", null), 0L);
                            return timestamp >= minTimestamp && timestamp <= maxTimestamp && value >= minValue && value <= maxValue;
                        })
                        .sorted(Comparator.comparingLong(input -> {
                            Node transaction = input.getOtherNode(lastNode);
                            return SafeConvert.toLong(transaction.getProperty("timestamp", null), 0L);
                        }))
                        .limit(maxRelationshipCount)
                        .forEach(output -> relationships.add(output));
                }
            } else if (lastNode.hasLabel(TRANSACTION)) {
                lastNode.getRelationships(INCOMING, INPUT).stream().parallel()
                    .filter(input -> {
                        long value = SafeConvert.toLong(input.getProperty("value", null), 0L);
                        return value >= minValue && value <= maxValue;
                    })
                    .sorted(Comparator.comparingLong(input -> SafeConvert.toLong(input.getProperty("value", null), 0L)))
                    .limit(maxRelationshipCount)
                    .forEach(input -> relationships.add(input));
            }

            return relationships.iterator();
        } catch (Exception e) {
            logger.warning("inbound" + lastNode.toString() + e.getMessage());
            return relationships.iterator();
        }
    }

    private Iterator<Relationship> outbound(
        long maxRelationshipCount, 
        long minTimestamp, 
        long maxTimestamp, 
        long minValue, 
        long maxValue, 
        Node lastNode,
        long timeout
    ) {
        List<Relationship> relationships = new ArrayList<>();

        try {
            if (System.currentTimeMillis() > timeout) return relationships.iterator();
            
            if (lastNode.hasLabel(ACCOUNT)) {
                if (lastNode.getDegree() < EXCHANGE_THRESHOLD) {
                    lastNode.getRelationships(OUTGOING, INPUT).stream().parallel()
                        .filter(input -> {
                            Node transaction = input.getOtherNode(lastNode);
                            long timestamp = SafeConvert.toLong(transaction.getProperty("timestamp", null), 0L);
                            long value = SafeConvert.toLong(transaction.getProperty("input_value", null), 0L);
                            return timestamp >= minTimestamp && timestamp <= maxTimestamp && value >= minValue && value <= maxValue;
                        })
                        .sorted(Comparator.comparingLong(input -> {
                            Node transaction = input.getOtherNode(lastNode);
                            return SafeConvert.toLong(transaction.getProperty("timestamp", null), 0L);
                        }))
                        .limit(maxRelationshipCount)
                        .forEach(input -> relationships.add(input));
                }
            } else if (lastNode.hasLabel(TRANSACTION)) {
                lastNode.getRelationships(OUTGOING, OUTPUT).stream().parallel()
                    .filter(output -> {
                        long value = SafeConvert.toLong(output.getProperty("value", null), 0L);
                        return value >= minValue && value <= maxValue;
                    })
                    .sorted(Comparator.comparingLong(input -> SafeConvert.toLong(input.getProperty("value", null), 0L)))
                    .limit(maxRelationshipCount)
                    .forEach(output -> relationships.add(output));
            }

            return relationships.iterator();
        } catch (Exception e) {
            logger.warning("outbound" + lastNode.toString() + e.getMessage());
            return relationships.iterator();
        }
    }

    PathExpander<Void> createExpander(
        long timespan,
        long maxRelationshipCount, 
        long timestamp, 
        long minValue, 
        long maxValue, 
        boolean reverse, 
        long timeout
    ) {
        return new PathExpander<>() {
            @Override
            public ResourceIterable<Relationship> expand(Path path, BranchState<Void> state) {
                try {
                    if (System.currentTimeMillis() > timeout) return PathExpanderFactory.createEmptyIterable();

                    Iterator<Node> reversedNodes = path.reverseNodes().iterator();
                    Node lastNode = reversedNodes.hasNext() ? reversedNodes.next() : null;
                    if (lastNode == null) return PathExpanderFactory.createEmptyIterable();

                    Relationship lastRelationship = path.lastRelationship();
                    long nextMinTimestamp;
                    long nextMaxTimestamp;
                    long nextMinValue;
                    long nextMaxValue;
                    if (lastRelationship == null) {
                        nextMinTimestamp = timestamp;
                        nextMaxTimestamp = nextMinTimestamp + DAY_RANGE;
                        nextMinValue = minValue;
                        nextMaxValue = maxValue;
                    } else {
                        Node lastTransaction = (lastRelationship.isType(OUTPUT)) ? lastRelationship.getStartNode() : lastNode;
                        if (reverse) {
                            nextMaxTimestamp = SafeConvert.toLong(lastTransaction.getProperty("timestamp", null), 0L);
                            nextMinTimestamp = nextMaxTimestamp - timespan;
                        } else {
                            nextMinTimestamp = SafeConvert.toLong(lastTransaction.getProperty("timestamp", null), 0L);
                            nextMaxTimestamp = nextMinTimestamp + timespan;
                        }
                        nextMinValue = SafeConvert.toLong(lastRelationship.getProperty("value", null), 0L) / 10;
                        nextMaxValue = Long.MAX_VALUE;
                    }

                    return new ResourceIterable<>() {
                        @Override
                        public void close() {}

                        @Override
                        public ResourceIterator<Relationship> iterator() {
                            return new ResourceIterator<>() {
                                private final Iterator<Relationship> iterator = (reverse)
                                    ? inbound(maxRelationshipCount, nextMinTimestamp, nextMaxTimestamp, nextMinValue, nextMaxValue, lastNode, timeout)
                                    : outbound(maxRelationshipCount, nextMinTimestamp, nextMaxTimestamp, nextMinValue, nextMaxValue, lastNode, timeout);

                                @Override
                                public void close() {
                                }

                                @Override
                                public boolean hasNext() {
                                    return iterator.hasNext();
                                }

                                @Override
                                public Relationship next() {
                                    return iterator.next();
                                }
                            };
                        }
                    };
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                    return PathExpanderFactory.createEmptyIterable();
                }
            }

            @Override
            public PathExpander<Void> reverse() {
                return this;
            }
        };
    }

    List<Path> getUniqueLongestPaths(List<Path> allPaths) {
        List<Path> uniqueLongestPaths = new ArrayList<>();

        for (Path thisPath : allPaths) {
            if (thisPath.startNode() == null || thisPath.endNode() == null) continue;

            boolean isSubpath = false;

            for (Path otherPath : allPaths) {
                if (thisPath == otherPath) continue;

                if (otherPath.length() < thisPath.length()) continue;

                if (otherPath.toString().contains(thisPath.toString())) {
                    isSubpath = true;
                    break;
                }
            }

            if (!isSubpath) uniqueLongestPaths.add(thisPath);
        }

        return uniqueLongestPaths;
    }
}