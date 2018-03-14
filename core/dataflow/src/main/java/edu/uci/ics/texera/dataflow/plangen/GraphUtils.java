package edu.uci.ics.texera.dataflow.plangen;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.engine.Plan;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.PlanGenException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.texera.dataflow.join.Join;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * This class provides a set of helper functions that are commonly used in Graph.
 *
 * @author Ming Huang
 */
public class GraphUtils {

    /**
     * Updates the current plan and fetch the schema from an operator
     * @param operatorID, the ID of an operator
     * @param inputSchemas inputSchemas of the target operator
     * @return Schema, which includes the attributes setting of the operator
     */
    public static Schema getOperatorOutputSchema(String operatorID, IOperator currentOperator,
                                                 Map<String, List<Schema>> inputSchemas)
            throws PlanGenException, DataflowException {

        Schema outputSchema = null;
        if (currentOperator instanceof ISourceOperator) {
            outputSchema = currentOperator.transformToOutputSchema();
        } else if (inputSchemas.containsKey(operatorID)) {
            List<Schema> inputSchema = inputSchemas.get(operatorID);
            try {
                outputSchema = currentOperator.transformToOutputSchema(
                        inputSchema.toArray(new Schema[inputSchema.size()]));
            } catch (TexeraException e) {
                System.out.println(e.getMessage());
            }
        }
        return outputSchema;
    }

    /**
     * Given a graph, retrieve input schema for all operators
     * @param graph graph to be searched
     * @return a map with id as key and input schema as value
     */
    public static Map<String, List<Schema>> retrieveAllOperatorInputSchema(Graph graph) throws PlanGenException {

        Map<String, Set<String>> adjacencyList = graph.getAdjacentList();
        Map<String, IOperator> operatorObjectMap = graph.getOperatorObjectMap();

        checkGraphCyclicity(adjacencyList);

        Map<String, Integer> inEdgeCount = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry: adjacencyList.entrySet()) {
            inEdgeCount.putIfAbsent(entry.getKey(), 0);
            for (String to: entry.getValue()) {
                inEdgeCount.put(to, inEdgeCount.getOrDefault(to, 0)+1);
            }
        }

        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, IOperator> entry: operatorObjectMap.entrySet()) {
            if (entry.getValue() instanceof ISourceOperator) {
                queue.add(entry.getKey());
            }
        }

        Map<String, List<Schema>> inputSchemas = new HashMap<>();
        while (!queue.isEmpty()) {
            String origin = queue.poll();
            Schema curOutputSchema = getOperatorOutputSchema(origin, operatorObjectMap.get(origin), inputSchemas);

            if (curOutputSchema != null) {
                for (String destination: adjacencyList.get(origin)) {
                    if (!(operatorObjectMap.get(destination) instanceof ISink))
                        inputSchemas.computeIfAbsent(destination, k -> new ArrayList<>()).add(curOutputSchema);
                    inEdgeCount.put(destination, inEdgeCount.get(destination)-1);
                    if (inEdgeCount.get(destination) == 0)
                    {
                        inEdgeCount.remove(destination);
                        queue.offer(destination);
                    }
                }
            }
        }
        return inputSchemas;
    }

    /**
     * Builds and returns the query plan from the operator graph.
     *
     * @return the plan generated from the operator graph
     * @throws PlanGenException, if the operator graph is invalid.
     */
    public static Plan buildQueryPlan(Graph graph) throws PlanGenException {

        Map<String, Set<String>> adjacencyList = graph.getAdjacentList();
        Map<String, PredicateBase> operatorPredicateMap = graph.getOperatorPredicateMap();
        Map<String, IOperator> operatorObjectMap = graph.getOperatorObjectMap();

        validateOperatorGraph(adjacencyList, operatorPredicateMap);
        connectOperators(adjacencyList, operatorObjectMap);

        ISink sink = findSinkOperator(adjacencyList, operatorPredicateMap, operatorObjectMap);

        Plan queryPlan = new Plan(sink);
        return queryPlan;
    }

    /*
     * Validates the operator graph.
     * The operator graph must meet all of the following requirements:
     *
     *   the graph is a DAG (directed acyclic graph)
     *     this DAG is weakly connected (no unreachable vertices).
     *     there's no cycles in this DAG.
     *   each operator must meet its input and output arity constraints.
     *   the operator graph has at least one source operator.
     *   the operator graph has exactly one sink.
     *
     * Throws PlanGenException if the operator graph is invalid.
     */
    private static void validateOperatorGraph(Map<String, Set<String>> adjacencyList,
                                       Map<String, PredicateBase> operatorPredicateMap) throws PlanGenException {
        checkGraphConnectivity(adjacencyList);
        checkGraphCyclicity(adjacencyList);
        checkOperatorInputArity(adjacencyList, operatorPredicateMap);
        checkOperatorOutputArity(adjacencyList, operatorPredicateMap);
        checkSourceOperator(adjacencyList, operatorPredicateMap);
        checkSinkOperator(adjacencyList, operatorPredicateMap);
    }

    /*
     * Detects if the graph can be partitioned disjoint subgraphs.
     *
     * This function builds an undirected version of the operator graph, and then
     *   uses a Depth First Search (DFS) algorithm to traverse the graph from any vertex.
     * If the graph is weakly connected, then every vertex should be reached after the traversal.
     *
     * PlanGenException is thrown if there is an operator not connected to the operator graph.
     *
     */
    private static void checkGraphConnectivity(Map<String, Set<String>> adjacencyList) throws PlanGenException {
        HashMap<String, HashSet<String>> undirectedAdjacencyList = new HashMap<>();
        for (String vertexOrigin : adjacencyList.keySet()) {
            undirectedAdjacencyList.put(vertexOrigin, new HashSet<>(adjacencyList.get(vertexOrigin)));
        }
        for (String vertexOrigin : adjacencyList.keySet()) {
            for (String vertexDestination : adjacencyList.get(vertexOrigin)) {
                undirectedAdjacencyList.get(vertexDestination).add(vertexOrigin);
            }
        }

        String vertex = undirectedAdjacencyList.keySet().iterator().next();
        HashSet<String> unvisitedVertices = new HashSet<>(undirectedAdjacencyList.keySet());

        connectivityDfsVisit(vertex, undirectedAdjacencyList, unvisitedVertices);

        if (! unvisitedVertices.isEmpty()) {
            throw new PlanGenException("Operators: " + unvisitedVertices + " are not connected to the operator graph.");
        }
    }

    /*
     * This is a helper function for checking connectivity by traversing the graph using DFS algorithm.
     */
    private static void connectivityDfsVisit(String vertex, HashMap<String, HashSet<String>> undirectedAdjacencyList,
                                      HashSet<String> unvisitedVertices) {
        unvisitedVertices.remove(vertex);
        for (String adjacentVertex : undirectedAdjacencyList.get(vertex)) {
            if (unvisitedVertices.contains(adjacentVertex)) {
                connectivityDfsVisit(adjacentVertex, undirectedAdjacencyList, unvisitedVertices);
            }
        }
    }

    /*
     * Detects if there are any cycles in the operator graph.
     *
     * This function uses a Depth First Search (DFS) algorithm to traverse the graph.
     * It detects a cycle by maintaining a list of visited vertices, and a list of DFS's path.
     * during the traversal, if it reaches an vertex that is in the DFS path, then there's a cycle.
     *
     * PlanGenException is thrown if a cycle is detected in the graph.
     *
     */
    private static void checkGraphCyclicity(Map<String, Set<String>> adjacencyList) throws PlanGenException {
        HashSet<String> unvisitedVertices = new HashSet<>(adjacencyList.keySet());
        HashSet<String> dfsPath = new HashSet<>();

        for (String vertex : adjacencyList.keySet()) {
            if (unvisitedVertices.contains(vertex)) {
                checkCyclicityDfsVisit(adjacencyList, vertex, unvisitedVertices, dfsPath);
            }
        }
    }

    /*
     * This is a helper function for detecting cycles by traversing the graph using a DFS algorithm.
     */
    private static void checkCyclicityDfsVisit(Map<String, Set<String>> adjacencyList,
                                               String vertex, HashSet<String> unvisitedVertices,
                                               HashSet<String> dfsPath) throws PlanGenException {
        unvisitedVertices.remove(vertex);
        dfsPath.add(vertex);

        for (String adjacentVertex : adjacencyList.get(vertex)) {
            if (dfsPath.contains(adjacentVertex)) {
                throw new PlanGenException("The following operators form a cycle in operator graph: " + dfsPath);
            }
            if (unvisitedVertices.contains(adjacentVertex)) {
                checkCyclicityDfsVisit(adjacencyList, adjacentVertex, unvisitedVertices, dfsPath);
            }
        }

        dfsPath.remove(vertex);
    }

    /*
     * Checks if the input arities of all operators match the expected input arities.
     */
    private static void checkOperatorInputArity(Map<String, Set<String>> adjacencyList,
                                                Map<String, PredicateBase> operatorPredicateMap) throws PlanGenException {

        HashMap<String, Integer> inputArityMap = new HashMap<>();
        for (String vertex : adjacencyList.keySet()) {
            inputArityMap.put(vertex, 0);
        }
        for (String vertexOrigin : adjacencyList.keySet()) {
            for (String vertexDestination : adjacencyList.get(vertexOrigin)) {
                int newCount = inputArityMap.get(vertexDestination) + 1;
                inputArityMap.put(vertexDestination, newCount);
            }
        }

        for (String vertex : inputArityMap.keySet()) {
            int actualInputArity = inputArityMap.get(vertex);
            int expectedInputArity = OperatorArityConstants.getFixedInputArity(
                    operatorPredicateMap.get(vertex).getClass());
            PlanGenUtils.planGenAssert(
                    actualInputArity == expectedInputArity,
                    String.format("Operator %s should have %d inputs, got %d.", vertex, expectedInputArity, actualInputArity));
        }
    }

    /*
     * Checks if the output arity of the operators matches.
     *
     * All operators (except sink) should have at least 1 output.
     *
     * The linking operator phase will automatically add a One to N Connector to
     * an operator with multiple outputs, so the output arities are not checked.
     *
     */
    private static void checkOperatorOutputArity(Map<String, Set<String>> adjacencyList,
                                                 Map<String, PredicateBase> operatorPredicateMap) throws PlanGenException {

        for (String vertex : adjacencyList.keySet()) {
            Class<? extends PredicateBase> predicateClass = operatorPredicateMap.get(vertex).getClass();

            int actualOutputArity = adjacencyList.get(vertex).size();
            int expectedOutputArity = OperatorArityConstants.getFixedOutputArity(predicateClass);

            if (predicateClass.toString().toLowerCase().contains("sink")) {
                PlanGenUtils.planGenAssert(
                        actualOutputArity == expectedOutputArity,
                        String.format("Sink %s should have %d output links, got %d.", vertex, expectedOutputArity, actualOutputArity));
            } else {
                PlanGenUtils.planGenAssert(
                        actualOutputArity != 0,
                        String.format("Operator %s should have at least %d output links, got 0.", vertex, expectedOutputArity));
            }
        }
    }

    /*
     * Checks that the operator graph has at least one source operator
     */
    private static void checkSourceOperator(Map<String, Set<String>> adjacencyList,
                                            Map<String, PredicateBase> operatorPredicateMap) throws PlanGenException {

        boolean sourceExist = adjacencyList.keySet().stream()
                .map(operator -> operatorPredicateMap.get(operator).getClass().toString())
                .anyMatch(predicateClass -> predicateClass.toLowerCase().contains("source"));

        PlanGenUtils.planGenAssert(sourceExist, "There must be at least one source operator.");
    }

    /*
     * Checks that the operator graph has exactly one sink operator.
     */
    private static void checkSinkOperator(Map<String, Set<String>> adjacencyList,
                                          Map<String, PredicateBase> operatorPredicateMap) throws PlanGenException {

        long sinkOperatorNumber = adjacencyList.keySet().stream()
                .map(operator -> operatorPredicateMap.get(operator).getClass().toString())
                .filter(predicateClass -> predicateClass.toLowerCase().contains("sink"))
                .count();

        PlanGenUtils.planGenAssert(sinkOperatorNumber == 1,
                String.format("There must be exaxtly one sink operator, got %d.", sinkOperatorNumber));
    }

    /*
     * Connects IOperator objects together according to the operator graph.
     *
     * This function assumes that the operator graph is valid.
     * It goes through every link, and invokes
     * the corresponding "setInputOperator" function to connect operators.
     */
    private static void connectOperators(Map<String, Set<String>> adjacencyList,
                                         Map<String, IOperator> operatorObjectMap) throws PlanGenException {

        for (String vertex : adjacencyList.keySet()) {
            IOperator currentOperator = operatorObjectMap.get(vertex);
            int outputArity = adjacencyList.get(vertex).size();

            // automatically adds a OneToNBroadcastConnector if the output arity > 1
            if (outputArity > 1) {
                OneToNBroadcastConnector oneToNConnector = new OneToNBroadcastConnector(outputArity);
                oneToNConnector.setInputOperator(currentOperator);
                int counter = 0;
                for (String adjacentVertex : adjacencyList.get(vertex)) {
                    IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
                    handleSetInputOperator(oneToNConnector.getOutputOperator(counter), adjacentOperator);
                    counter++;
                }
            } else {
                for (String adjacentVertex : adjacencyList.get(vertex)) {
                    IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
                    handleSetInputOperator(currentOperator, adjacentOperator);
                }
            }
        }
    }

    /*
     * Invoke the corresponding "setInputOperator" method of the dest operator.
     */
    private static void handleSetInputOperator(IOperator src, IOperator dest) throws PlanGenException {
        // handles Join operator differently
        if (dest instanceof Join) {
            Join join = (Join) dest;
            if (join.getInnerInputOperator() == null) {
                join.setInnerInputOperator(src);
            } else {
                join.setOuterInputOperator(src);
            }
            // invokes "setInputOperator" for all other operators
        } else {
            try {
                dest.getClass().getMethod("setInputOperator", IOperator.class).invoke(dest, src);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                    | IllegalArgumentException | InvocationTargetException e) {
                throw new PlanGenException(e.getMessage(), e);
            }
        }
    }

    /*
     * Finds the sink operator in the operator graph.
     *
     * This function assumes that the graph is valid and there is only one sink in the graph.
     */
    private static ISink findSinkOperator(Map<String, Set<String>> adjacencyList,
                                          Map<String, PredicateBase> operatorPredicateMap,
                                          Map<String, IOperator> operatorObjectMap) throws PlanGenException {

        IOperator sinkOperator = adjacencyList.keySet().stream()
                .filter(operator -> operatorPredicateMap.get(operator)
                        .getClass().toString().toLowerCase().contains("sink"))
                .map(operator -> operatorObjectMap.get(operator))
                .findFirst().orElse(null);

        PlanGenUtils.planGenAssert(sinkOperator != null, "Error: sink operator doesn't exist.");
        PlanGenUtils.planGenAssert(sinkOperator instanceof ISink, "Error: sink operator's type doesn't match.");

        return (ISink) sinkOperator;
    }
}
