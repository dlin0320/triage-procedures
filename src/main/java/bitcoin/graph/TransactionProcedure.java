package bitcoin.graph;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.procedure.*;
import utility.SafeConvert;
import utility.Model.TransactionsAndLabels;
import java.util.*;
import java.util.stream.Stream;
import static org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_GLOBAL;

public class TransactionProcedure extends GraphProcedure {
    @Procedure(name = "bitcoin.graph.transaction", mode = Mode.READ)
    @Description("Performs a custom traversal on the graph and return the transactions.")
    public Stream<TransactionsAndLabels> getTransactions(
        @Name("startNodeAddress") String startNodeAddress,
        @Name("timespan") long timespan,
        @Name("maxRelationshipCount") long maxRelationshipCount,
        @Name("minTimestamp") long startTimestamp,
        @Name("minValue") long minValue,
        @Name("maxValue") long maxValue,
        @Name("reverse") boolean reverse
    ) {
        try {
            Node startNode = tx.findNode(ACCOUNT, "address", startNodeAddress);

			PathEvaluator<Object> depthEvaluator = (reverse) ? Evaluators.toDepth(2) : Evaluators.includingDepths(2, 14);

            long timeout = System.currentTimeMillis() + MAX_SEARCH_TIME;

            final TraversalDescription traversalDescription = tx.traversalDescription()
                .breadthFirst()
                .uniqueness(RELATIONSHIP_GLOBAL)
                .expand(createExpander(timespan, maxRelationshipCount, startTimestamp, minValue, maxValue, reverse, timeout))
                .evaluator(depthEvaluator);

            List<Path> allPaths = new ArrayList<>();

            for (Path path : traversalDescription.traverse(startNode)) {
                allPaths.add(path);
            }

            List<Path> uniqueLongestPaths = getUniqueLongestPaths(allPaths);

            Map<String, Object> transactions = new HashMap<>();
            Map<String, Object> labels = new HashMap<>();

            uniqueLongestPaths.forEach(path -> {
                if (path.endNode().hasLabel(ACCOUNT)) {
                    Iterator<Node> nodeIterator = path.nodes().iterator();
                    while (nodeIterator.hasNext()) {
                        Node node = nodeIterator.next();
                        if (node.hasLabel(ACCOUNT)) {
                            String address = SafeConvert.toString(node.getProperty("address", null), "");
                            HashSet<String> existingLabels = (HashSet<String>) labels.get(address);
                            String label = node.getDegree() > EXCHANGE_THRESHOLD ? "deposit" : null;
                            if (label == null) continue;

                            if (existingLabels != null) {
                                existingLabels.add(label);
                                labels.put(address, existingLabels);
                            } else {
                                HashSet<String> newLabel = new HashSet<String>();
                                newLabel.add(label);
                                labels.put(address, newLabel);
                            }
                        }
                    }

                    Iterator<Relationship> relationshipIterator = path.relationships().iterator();
                    while (relationshipIterator.hasNext()) {
                        Relationship firstRel = relationshipIterator.next();
                        Relationship secondRel = relationshipIterator.hasNext() ? relationshipIterator.next() : null;
                        if (secondRel == null) break;

                        if (firstRel != null) {
                            Node from;
                            Node to;
                            Node transaction;
                            Relationship input;
                            Relationship output;

                            if (reverse) {
                                from = secondRel.getStartNode();
                                to = firstRel.getEndNode();
                                transaction = firstRel.getStartNode();
                                input = secondRel;
                                output = firstRel;
                            } else {
                                from = firstRel.getStartNode();
                                to = secondRel.getEndNode();
                                transaction = firstRel.getEndNode();
                                input = firstRel;
                                output = secondRel;
                            }

                            String hash = SafeConvert.toString(transaction.getProperty("hash", null), "");
                            long timestamp = SafeConvert.toLong(transaction.getProperty("timestamp", null), 0L);

                            if (transactions.containsKey(transaction.getElementId())) {
                                Map<String, Object> existingTransaction = (Map<String, Object>) transactions.get(transaction.getElementId());
                                Set<Map<String, String>> inputs = (Set<Map<String, String>>) existingTransaction.get("inputs");
                                Set<Map<String, String>> outputs = (Set<Map<String, String>>) existingTransaction.get("outputs");
                                inputs.add(new HashMap<String, String>() {{
                                    put("from", SafeConvert.toString(from.getProperty("address", null), ""));
                                    put("value", SafeConvert.toString(input.getProperty("value", null), "0"));
                                }});
                                outputs.add(new HashMap<String, String>() {{
                                    put("to", SafeConvert.toString(to.getProperty("address", null), ""));
                                    put("value", SafeConvert.toString(output.getProperty("value", null), "0"));
                                }});
                            } else {
                                transactions.put(transaction.getElementId(), new HashMap<>() {{
                                    put("hash", hash);
                                    put("timestamp", timestamp);
                                    put("inputs", new HashSet<Map<String, String>>() {{
                                        add(new HashMap<String, String>() {{
                                            put("from", SafeConvert.toString(from.getProperty("address", null), ""));
                                            put("value", SafeConvert.toString(input.getProperty("value", null), "0"));
                                        }});
                                    }});
                                    put("outputs", new HashSet<Map<String, String>>() {{
                                        add(new HashMap<String, String>() {{
                                            put("to", SafeConvert.toString(to.getProperty("address", null), ""));
                                            put("value", SafeConvert.toString(output.getProperty("value", null), "0"));
                                        }});
                                    }});
                                }});
                            }

                        }
                    }
                }
            });

            return Stream.of(new TransactionsAndLabels(transactions, labels));
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Stream.empty();
        }
    }
}