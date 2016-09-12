package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.plangen.PlanGenUtils;
import edu.uci.ics.textdb.storage.DataStore;

/**
 * KeywordSourceBuilder provides a static function that builds a KeywordMatcherSourceOperator.
 * 
 * KeywordSourceBuilder currently needs the following properties:
 * 
 *   keyword (required)
 *   matchingType (required)
 *   
 *   properties required for constructing attributeList, see OperatorBuilderUtils.constructAttributeList
 *   properties required for constructing dataStore, see OperatorBuilderUtils.constructDataStore
 * 
 * 
 * @author Zuozhi Wang
 *
 */
public class KeywordSourceBuilder {
    
    public static String KEYWORD = KeywordMatcherBuilder.KEYWORD;
    public static final String MATCHING_TYPE = KeywordMatcherBuilder.MATCHING_TYPE;

    
    public static KeywordMatcherSourceOperator buildSourceOperator(Map<String, String> operatorProperties) throws PlanGenException, DataFlowException {
        String keyword = OperatorBuilderUtils.getRequiredProperty(KEYWORD, operatorProperties);
        String matchingTypeStr = OperatorBuilderUtils.getRequiredProperty(MATCHING_TYPE, operatorProperties);

        // check if keyword is empty
        PlanGenUtils.planGenAssert(!keyword.trim().isEmpty(), "keyword is empty");

        // generate attribute list
        List<Attribute> attributeList = OperatorBuilderUtils.constructAttributeList(operatorProperties);

        // generate matching type
        KeywordMatchingType matchingType = KeywordMatcherBuilder.getKeywordMatchingType(matchingTypeStr);
        PlanGenUtils.planGenAssert(matchingType != null, 
                "matching type: "+ matchingTypeStr +" is not valid, "
                + "must be one of " + KeywordMatcherBuilder.keywordMatchingTypeMap.keySet());
        
        KeywordPredicate keywordPredicate = new KeywordPredicate(keyword, attributeList,
                DataConstants.getStandardAnalyzer(), matchingType);
        
        DataStore dataStore = OperatorBuilderUtils.constructDataStore(operatorProperties);

        KeywordMatcherSourceOperator sourceOperator = new KeywordMatcherSourceOperator(keywordPredicate, dataStore);
   
        return sourceOperator;
    }

}
