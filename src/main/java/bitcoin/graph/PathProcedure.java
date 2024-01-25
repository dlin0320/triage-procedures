package bitcoin.graph;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.procedure.*;
import utility.Model.PathResult;
import java.util.*;
import java.util.stream.Stream;
import static org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_GLOBAL;

public class PathProcedure extends GraphProcedure{
	@Procedure(name = "bitcoin.graph.path", mode = Mode.READ)
	@Description("Performs a custom traversal on the graph and return the paths.")
	public Stream<PathResult> getPaths(
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

			PathEvaluator<Object> depthEvaluator = reverse ? Evaluators.toDepth(2) : Evaluators.includingDepths(2, 14);

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

			return uniqueLongestPaths.stream().map(PathResult::new);
		} catch (Exception e) {
			logger.severe(e.getMessage());
			return Stream.empty();
		}
	}
}