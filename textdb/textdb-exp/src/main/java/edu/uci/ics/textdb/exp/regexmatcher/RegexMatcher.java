package edu.uci.ics.textdb.exp.regexmatcher;

import java.lang.reflect.Array;
import java.util.*;

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
    //(<[^<>]*>[+]*)

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
        long totalTime = 0;
        long start = System.currentTimeMillis();
        if (inputTuple == null) {
            return null;
        }

        if(idLabelMapping.size() == 0) {
        	// Unlabelled regex
        	return processUnlabelledRegex(inputTuple);
        }
        // else labelled regex


        long curtime = System.currentTimeMillis();
        totalTime += curtime - start;
        System.out.println(totalTime);
        return processLabelledRegex(inputTuple);

    }
    
    private Tuple processLabelledRegex(Tuple inputTuple) {
    	HashMap<Integer, List<List<Span>>> labelSpanList = createLabelledSpanList(inputTuple, idLabelMapping);
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

    
    private String extractLabels(String generalRegexPattern, HashMap<Integer, HashSet<String>> idLabelMapping) {
    	java.util.regex.Pattern patt = java.util.regex.Pattern.compile(labelSyntax, 
                java.util.regex.Pattern.CASE_INSENSITIVE);
    	java.util.regex.Matcher match = patt.matcher(generalRegexPattern);

    	int id = 1;
    	String regexMod = generalRegexPattern;
    	while(match.find()) {
            int start = match.start();
            int end = match.end();

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
    
	private ArrayList<String> generateAllCombinationsOfRegex(String genericRegex, HashMap<Integer, List<List<Span>>> labelSpanList) {
    	ArrayList<String> allRegexCombinations = new ArrayList<String>();
    	String regEx = "<([0-9]+)>";
    	java.util.regex.Matcher m = java.util.regex.Pattern.compile(regEx).matcher(genericRegex);
    	List<Integer> pos = new ArrayList<Integer>();
    	List<Integer> matchedValues = new ArrayList<Integer>();
    	List<List<List<Span>>> listOfSets = new ArrayList<>();
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

		List<List<Span>> cartesianResult = cartesianProduct(listOfSets);

		allRegexCombinations = getRegexCombinations(genericRegex, labelSpanList, cartesianResult);

    	return allRegexCombinations;
    }

    public List<List<Span>> cartesianProduct(List<List<List<Span>>> lists) {
	    List<List<Span>> resultLists = new ArrayList<List<Span>>();
	    if (lists.size() == 0) {
	        resultLists.add(new ArrayList<Span>());
	        return resultLists;
	    } 
	    else {
	        List<List<Span>> firstSet = lists.get(0);
	        List<List<Span>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
	        for(List<Span> list: firstSet){
	        for (Span span : list) {
	            for (List<Span> remainingList : remainingLists) {
	                ArrayList<Span> resultList = new ArrayList<Span>();
	                resultList.add(span);
	                resultList.addAll(remainingList);
	                resultLists.add(resultList);
	            }
	        }
	    }

        }

        resultLists.removeIf(result -> !validCombination(result));
	    return resultLists;
	}

	private static ArrayList<String> getRegexCombinations(String regex, HashMap<Integer, List<List<Span>>> labelSpanList, List<List<Span>> stringReplacements) {
		ArrayList<String> resultArray = new ArrayList<String>();

		String regExpression;
		String pattern = ""; 
		String resultStr = "";

    	for(List<Span> words: stringReplacements) {
    		regExpression = regex;
    		for(Integer key: labelSpanList.keySet()) {
    			pattern = "<" + key + ">";
       			resultStr = regExpression.replace(pattern, words.get(key - 1).getValue());
    			regExpression = resultStr;
    		}
    		resultArray.add(regExpression);
    	}
		return resultArray;
		
	}
	private boolean validCombination(List<Span> list){
        for(int i = 0; i < list.size()-1; i++){
            if(list.get(i+1).getStart() - list.get(i).getEnd() < 0){
                return false;
            }
        }
        return true;
    }
    
    
   // private HashMap<Integer, HashSet<String>> createLabelledSpanList(Tuple inputTuple, HashMap<Integer, HashSet<String>> idLabelMapping) {
   // 	HashMap<Integer, HashSet<String>> labelSpanList = new HashMap<Integer, HashSet<String>>();
    //    for (int id : idLabelMapping.keySet()) {
     //       HashSet<String> labels = idLabelMapping.get(id);
     //       HashSet<String> values = new HashSet<String>();
     //       for (String oneField : labels) {
     //           ListField<Span> spanListField = inputTuple.getField(oneField);
     //           List<Span> spanList = spanListField.getValue();
     //           for (Span span : spanList) {
     //               values.add(span.getValue());
     //           }
     //       }
     //       labelSpanList.put(id, values);
     //   }
     //   return labelSpanList;
   // }

    private HashMap<Integer, List<List<Span>>> createLabelledSpanList(Tuple inputTuple, HashMap<Integer, HashSet<String>> idLabelMapping) {
        HashMap<Integer, List<List<Span>>> labelSpanList = new HashMap<>();
        for (int id : idLabelMapping.keySet()) {
            HashSet<String> labels = idLabelMapping.get(id);
            List<List<Span>> values = new ArrayList<>();
            //HashSet<String> values = new HashSet<String>();
            for (String oneField : labels) {
                ListField<Span> spanListField = inputTuple.getField(oneField);
                List<Span> spanList = spanListField.getValue();
                values.add(spanList);
            }
            labelSpanList.put(id, values);
        }
        return labelSpanList;
    }
}
