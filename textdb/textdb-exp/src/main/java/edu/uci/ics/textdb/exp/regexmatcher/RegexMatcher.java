package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

/**
 * Created by chenli on 3/25/16.
 * 
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 */
public class RegexMatcher extends AbstractSingleInputOperator {
	
    private final RegexPredicate predicate;
    private static final String labelSyntax = "<[^<>]*>";
    
    // two available regex engines, RegexMatcher will try RE2J first
    private enum RegexEngine {
        JavaRegex, RE2J
    }

    private RegexEngine regexEngine;
    private com.google.re2j.Pattern re2jPattern;
    private java.util.regex.Pattern javaPattern;
    private HashMap<Integer, HashSet<String>> idLabelMapping;
    private String regexMod;
    
    private Schema inputSchema;

    public RegexMatcher(RegexPredicate predicate) {
        this.predicate = predicate;
    }
    
    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        idLabelMapping = new HashMap<Integer, HashSet<String>>();
        regexMod = extractLabels(predicate.getRegex(), idLabelMapping);
        if(idLabelMapping.size()==0) {
        	// No labels in regex
        	compileRegexPattern(predicate.getRegex());
        }
        if (!this.inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            outputSchema = Utils.createSpanSchema(inputSchema);
        }
    }
    
    
    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;
        
        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
                inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
            }            
            resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                break;
            }
        }
        
        return resultTuple;
    }

    /**
     * This function returns a list of spans in the given tuple that match the
     * regex For example, given tuple ("george watson", "graduate student", 23,
     * "(949)888-8888") and regex "g[^\s]*", this function will return
     * [Span(name, 0, 6, "g[^\s]*", "george watson"), Span(position, 0, 8,
     * "g[^\s]*", "graduate student")]
     * 
     * @param inputTuple
     *            document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence
     *         in the document
     * @throws DataFlowException
     */
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) {
        if (inputTuple == null) {
            return null;
        }

        if(idLabelMapping.size() == 0) {
        	// Unlabelled regex
        	return processUnlabelledRegex(inputTuple);
        }
        // else labelled regex
        return processLabelledRegex(inputTuple);

    }
    
    private Tuple processLabelledRegex(Tuple inputTuple) {
    	HashMap<Integer, HashSet<String>> labelSpanList = createLabelledSpanList(inputTuple, idLabelMapping);
    	ArrayList<String> allRegexCombincations = generateAllCombinationsOfRegex(regexMod, labelSpanList);
        ListField<Span> spanListField = inputTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList = spanListField.getValue();
    	for(String regex : allRegexCombincations) {
    		compileRegexPattern(regex);
    		spanList.addAll(labelledRegexMatcher(inputTuple));
    	}
    	if (spanList.isEmpty()) {
            return null;
        }
    	
        return inputTuple;
    }
    
    private Tuple processUnlabelledRegex(Tuple inputTuple) {
      List<Span> matchingResults = new ArrayList<>();

      for (String attributeName : predicate.getAttributeNames()) {
          AttributeType attributeType = inputSchema.getAttribute(attributeName).getAttributeType();
          String fieldValue = inputTuple.getField(attributeName).getValue().toString();

          // types other than TEXT and STRING: throw Exception for now
          if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
              throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
          }

          switch (regexEngine) {
          case JavaRegex:
              matchingResults.addAll(javaRegexMatch(fieldValue, attributeName));
              break;
          case RE2J:
              matchingResults.addAll(re2jRegexMatch(fieldValue, attributeName));
              break;
          }
      }

      if (matchingResults.isEmpty()) {
          return null;
      }

      ListField<Span> spanListField = inputTuple.getField(SchemaConstants.SPAN_LIST);
      List<Span> spanList = spanListField.getValue();
      spanList.addAll(matchingResults);

      return inputTuple;
    }

    private List<Span> javaRegexMatch(String fieldValue, String attributeName) {
        List<Span> matchingResults = new ArrayList<>();
        java.util.regex.Matcher javaMatcher = this.javaPattern.matcher(fieldValue);
        while (javaMatcher.find()) {
            int start = javaMatcher.start();
            int end = javaMatcher.end();
            matchingResults.add(
                    new Span(attributeName, start, end, this.predicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }

    private List<Span> re2jRegexMatch(String fieldValue, String attributeName) {
        List<Span> matchingResults = new ArrayList<>();
        com.google.re2j.Matcher re2jMatcher = this.re2jPattern.matcher(fieldValue);
        while (re2jMatcher.find()) {
            int start = re2jMatcher.start();
            int end = re2jMatcher.end();
            matchingResults.add(
                    new Span(attributeName, start, end, this.predicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }

    @Override
    protected void cleanUp() throws DataFlowException {        
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }
    
    /**
     * Compile regex pattern
     * @param regex
     */
    private void compileRegexPattern(String regex) {
        // try Java Regex first
        try {
            if (this.predicate.isIgnoreCase()) {
                this.javaPattern = java.util.regex.Pattern.compile(regex, 
                        java.util.regex.Pattern.CASE_INSENSITIVE);
                this.regexEngine = RegexEngine.JavaRegex; 
            } else {
                this.javaPattern = java.util.regex.Pattern.compile(regex);
                this.regexEngine = RegexEngine.JavaRegex; 
            }

            // if Java Regex fails, try RE2J
        } catch (java.util.regex.PatternSyntaxException javaException) {
            try {
                if (this.predicate.isIgnoreCase()) {
                    this.re2jPattern = com.google.re2j.Pattern.compile(regex, 
                            com.google.re2j.Pattern.CASE_INSENSITIVE);
                    this.regexEngine = RegexEngine.RE2J;
                } else {
                    this.re2jPattern = com.google.re2j.Pattern.compile(regex);
                    this.regexEngine = RegexEngine.RE2J;                    
                }

                // if RE2J also fails, throw exception
            } catch (com.google.re2j.PatternSyntaxException re2jException) {
                throw new DataFlowException(javaException.getMessage(), javaException);
            }
        }
    }

    /**
     * Extracts labels from regex if present
     * @param generalRegexPattern
     * @param idLabelMapping
     * @return modified regex haveing id instead of labels
     */
    private static String extractLabels(String generalRegexPattern, HashMap<Integer, HashSet<String>> idLabelMapping) {
    	java.util.regex.Pattern patt = java.util.regex.Pattern.compile(labelSyntax, 
                java.util.regex.Pattern.CASE_INSENSITIVE);
    	java.util.regex.Matcher match = patt.matcher(generalRegexPattern);

    	int id = 1;
    	String regexMod = generalRegexPattern;
    	while(match.find()) {
            int start = match.start();
            int end = match.end();

            if(start>0 && generalRegexPattern.charAt(start-1)=='\\' && generalRegexPattern.charAt(end-2)=='\\')
                continue;

            String substr = generalRegexPattern.substring(start+1, end-1);
            String substrWithoutSpace = substr.replaceAll("\\s+", "");

            idLabelMapping.put(id, new HashSet<String>());
            
            if(substrWithoutSpace.contains("|")) {
            	// Multiple value separated by OR operator
            	String[] sublabs = substrWithoutSpace.split("[|]");
            	for(String lab : sublabs)
            		idLabelMapping.get(id).add(lab);
            } else {
            	idLabelMapping.get(id).add(substrWithoutSpace);
            }
            regexMod = regexMod.replace("<"+substr+">", "<"+id+">");
            id++;
    	}
    	return regexMod;
    }
    
    /**
     * Regex matcher function for labelled regex
     * @param inputTuple
     * @return Return list of span
     * @throws DataFlowException
     */
    private List<Span> labelledRegexMatcher(Tuple inputTuple) throws DataFlowException {
        if (inputTuple == null) {
            return null;
        }

        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            switch (regexEngine) {
            case JavaRegex:
                matchingResults.addAll(javaRegexMatch(fieldValue, attributeName));
                break;
            case RE2J:
                matchingResults.addAll(re2jRegexMatch(fieldValue, attributeName));
                break;
            }
        }
        return matchingResults;
    }
    
    /**
     * Generates all possible combinations of labels for this tuple.
     * @param genericRegex
     * @param labelSpanList
     * @return List of regex patterns 
     */
	private ArrayList<String> generateAllCombinationsOfRegex(String genericRegex, HashMap<Integer, HashSet<String>> labelSpanList) {
    	ArrayList<String> allRegexCombinations = new ArrayList<String>();
    	String regEx = "<([0-9]+)>";
    	java.util.regex.Matcher m = java.util.regex.Pattern.compile(regEx).matcher(genericRegex);
    	List<Integer> pos = new ArrayList<Integer>();
    	List<Integer> matchedValues = new ArrayList<Integer>();
    	List<HashSet<String>> listOfSets = new ArrayList<HashSet<String>>();
    	int matchedValue = 0;
		while (m.find())
		{
		    pos.add(m.start());
		    matchedValue = Integer.parseInt(m.group(1));
		    if(labelSpanList.containsKey(matchedValue)){
		    	matchedValues.add(matchedValue);
		    	listOfSets.add(labelSpanList.get(matchedValue));
		    }
		}

		List<List<String>> cartesianResult = cartesianProduct(listOfSets);

		allRegexCombinations = getRegexCombinations(genericRegex, labelSpanList, cartesianResult);

    	return allRegexCombinations;
    }

	/**
	 * Finds the Cartesian product of all sets of label values
	 * @param lists
	 * @return
	 */
    public static List<List<String>> cartesianProduct(List<HashSet<String>> lists) {
	    List<List<String>> resultLists = new ArrayList<List<String>>();
	    if (lists.size() == 0) {
	        resultLists.add(new ArrayList<String>());
	        return resultLists;
	    } 
	    else {
	        HashSet<String> firstSet = lists.get(0);
	        List<List<String>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
	        for (String condition : firstSet) {
	            for (List<String> remainingList : remainingLists) {
	                ArrayList<String> resultList = new ArrayList<String>();
	                resultList.add(condition);
	                resultList.addAll(remainingList);
	                resultLists.add(resultList);
	            }
	        }
	    }
	    return resultLists;
	}

    /**
     * Takes all the combinations across the labels and makes a universal
     * list of all the regexs that will have to be compiled and tested 
     * for one tuple.
     * @param regex
     * @param labelSpanList
     * @param stringReplacements
     * @return List of all the valid regex patterns for this tuple
     */
	public static ArrayList<String> getRegexCombinations(String regex, HashMap<Integer, HashSet<String>> labelSpanList, List<List<String>> stringReplacements) {
		ArrayList<String> resultArray = new ArrayList<String>();

		String regExpression;
		String pattern = ""; 
		String resultStr = "";

    	for(List<String> words: stringReplacements) {
    		regExpression = regex;
    		for(Integer key: labelSpanList.keySet()) {
    			pattern = "<" + key + ">";
       			resultStr = regExpression.replace(pattern, words.get(key - 1));
    			regExpression = resultStr;
    		}
    		resultArray.add(regExpression);
    	}
		return resultArray;
		
	}
    
    /**
     * Extracts the values for the required span lists and returns a map of 
     * each label to all its possible values.
     * @param inputTuple
     * @param idLabelMapping
     * @return Map of label id to all values
     */
    private HashMap<Integer, HashSet<String>> createLabelledSpanList(Tuple inputTuple, HashMap<Integer, HashSet<String>> idLabelMapping) {
    	HashMap<Integer, HashSet<String>> labelSpanList = new HashMap<Integer, HashSet<String>>();
        for (int id : idLabelMapping.keySet()) {
            HashSet<String> labels = idLabelMapping.get(id);
            HashSet<String> values = new HashSet<String>();
            for (String oneField : labels) {
                ListField<Span> spanListField = inputTuple.getField(oneField);
                List<Span> spanList = spanListField.getValue();
                for (Span span : spanList) {
                    values.add(span.getValue());
                }
            }
            labelSpanList.put(id, values);
        }
        return labelSpanList;
    }

}
