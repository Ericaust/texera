package edu.uci.ics.texera.dataflow.plangen;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.PlanGenException;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

import java.util.*;

/**
 * A graph of operators representing a query plan.
 *
 * @author Ming Huang
 */

public class Graph {


    // Variable indicates whether the plan has been changed
    private boolean UPDATED = true;
    // a map from operatorID to its operator
    private Map<String, IOperator> operatorObjectMap;
    // use LinkedHashMap to retain insertion order
    // a map from operatorID to its predicate
    private Map<String, PredicateBase> operatorPredicateMap;
    // a map of an operator ID to operator's outputs (a set of operator IDs)
    private Map<String, Set<String>> adjacencyList;

    /**
     * Create an empty graph.
     *
     * This class is not a JSON entry point. It is for internal use only.
     */
    public Graph() {
        operatorPredicateMap = new LinkedHashMap<>();
        adjacencyList = new LinkedHashMap<>();
    }

    /**
     * Create a LogicalPlan from an existing plan (represented by a list of operators and a list of links)
     *
     * @param predicateList, a list of operator predicates
     * @param operatorLinkList, a list of operator links
     */
    @JsonCreator
    public Graph(
            @JsonProperty(value = PropertyNameConstants.OPERATOR_LIST, required = true)
                    List<PredicateBase> predicateList,
            @JsonProperty(value = PropertyNameConstants.OPERATOR_LINK_LIST, required = true)
                    List<OperatorLink> operatorLinkList
    ) {
        // initialize private variables
        this();
        // add predicates and links
        for (PredicateBase predicate : predicateList) {
            addOperator(predicate);
        }
        for (OperatorLink link : operatorLinkList) {
            addLink(link);
        }
        buildOperators();
    }

    /**
     * Gets the operator predicate map.
     * @return a map of operator predicates
     */
    public Map<String, PredicateBase> getOperatorPredicateMap() {
        return operatorPredicateMap;
    }

    /**
     * Gets the operator object map.
     * @return a map of operator predicates
     */
    public Map<String, IOperator> getOperatorObjectMap() {
        if (UPDATED)
            buildOperators();
        return operatorObjectMap;
    }

    /**
     * Gets the adjacent list.
     * @return a list of operator links
     */
    public Map<String, Set<String>> getAdjacentList() {
        return adjacencyList;
    }

    /**
     * Adds a new operator to the graph.
     * @param operatorPredicate, the predicate of the operator
     */
    private void addOperator(PredicateBase operatorPredicate) {
        UPDATED = true;

        String operatorID = operatorPredicate.getID();
        PlanGenUtils.planGenAssert(! hasOperator(operatorID),
                String.format("duplicate operator id: %s is found", operatorID));
        operatorPredicateMap.put(operatorID, operatorPredicate);
        adjacencyList.put(operatorID, new LinkedHashSet<>());
    }

    /**
     * Adds a new link to the graph
     * @param operatorLink, a link of two operators
     */
    private void addLink(OperatorLink operatorLink) {
        UPDATED = true;

        String origin = operatorLink.getOrigin();
        String destination = operatorLink.getDestination();
        PlanGenUtils.planGenAssert(hasOperator(origin),
                String.format("origin operator id: %s is not found", origin));
        PlanGenUtils.planGenAssert(hasOperator(destination),
                String.format("destination operator id: %s is not found", destination));
        adjacencyList.get(origin).add(destination);
    }

    /**
     * Returns true if the operator graph contains the operatorID.
     *
     * @param operatorID ID of the operator to be checked
     * @return true if contains, false otherwise
     */
    private boolean hasOperator(String operatorID) {
        return adjacencyList.containsKey(operatorID);
    }

    /*
     * Build the operator objects from operator properties.
     */
    private void buildOperators() throws PlanGenException {
        operatorObjectMap = new HashMap<>();
        for (String operatorID : operatorPredicateMap.keySet()) {
            IOperator operator = operatorPredicateMap.get(operatorID).newOperator();
            operatorObjectMap.put(operatorID, operator);
        }
        UPDATED = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Graph that = (Graph) o;

        if (operatorPredicateMap != null ? !operatorPredicateMap.equals(that.operatorPredicateMap) : that.operatorPredicateMap != null)
            return false;
        return adjacencyList != null ? adjacencyList.equals(that.adjacencyList) : that.adjacencyList == null;

    }

    @Override
    public int hashCode() {
        int result = operatorPredicateMap != null ? operatorPredicateMap.hashCode() : 0;
        result = 31 * result + (adjacencyList != null ? adjacencyList.hashCode() : 0);
        return result;
    }
}
