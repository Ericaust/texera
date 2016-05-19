package edu.uci.ics.textdb.dataflow.regexmatch;

import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;

/**
 * This class translates a regex to a boolean query of n-grams,
 * according to the <a href='https://swtch.com/~rsc/regexp/regexp4.html'>algorithm</a> 
 * described in Russ Cox's article. <br>
 * 
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 */
public class RegexToGramQueryTranslator {	
	/**
	 * This method translates a regular expression to 
	 * a boolean expression of n-grams. <br>
	 * Then the boolean expression can be queried using 
	 * an n-gram inverted index to speed up regex matching. <br>
	 * 
	 * 
	 * @param regex, the regex string to be translated.
	 * @return GamBooleanQeruy, a boolean query of n-grams.
	 */
	public static GramBooleanQuery translate(String regex) 
		throws com.google.re2j.PatternSyntaxException{
		
	    PublicRegexp re = PublicParser.parse(regex, PublicRE2.PERL);
	    re = PublicSimplify.simplify(re);
	    RegexInfo regexInfo = analyze(re);
	    return regexInfo.match;

	}
	
	
	/**
	 * This is the main function of analyzing a regular expression. <br>
	 * This methods walks through the regex abstract syntax tree generated by RE2J, 
	 * and 
	 * 
	 * @param PublicRegexp
	 * @return RegexInfo
	 */
	private static RegexInfo analyze(PublicRegexp re) {
		RegexInfo info = new RegexInfo();
		switch (re.getOp()) {
		// NO_MATCH is a regex that doesn't match anything.
		// It's used to handle error cases, which shouldn't 
		// happen unless something goes wrong.
		case NO_MATCH: 
		case VERTICAL_BAR:
		case LEFT_PAREN: {
			return RegexInfo.matchNone();
		}
		// The following cases are treated as 
		// a regex that matches an empty string.
		case EMPTY_MATCH:
		case WORD_BOUNDARY:	case NO_WORD_BOUNDARY:
		case BEGIN_LINE: 	case END_LINE:
		case BEGIN_TEXT: 	case END_TEXT: {
			return RegexInfo.emptyString();
		}
		// A regex that matches any character
		case ANY_CHAR: case ANY_CHAR_NOT_NL: {
			return RegexInfo.anyChar();
		}
		// TODO finish for every case
		case ALTERNATE:
			//TODO
			return fold((x, y) -> concat(x,y), re.getSubs(), RegexInfo.matchAny());
		case CONCAT:
			return fold((x, y) -> alternate(x, y), re.getSubs(), RegexInfo.matchNone());
		case CAPTURE:
			return analyze(re.getSubs()[0]);
		// For example, [a-z]
		case CHAR_CLASS:
			boolean isCaseSensitive = (re.getFlags() & PublicRE2.FOLD_CASE) > 0;
			
			if (re.getRunes().length == 0) {
				return RegexInfo.matchNone();
			} else if (re.getRunes().length == 1) {
				String exactStr;
				if (isCaseSensitive) {
					exactStr = Character.toString((char) re.getRunes()[0]);
				} else {
					exactStr = Character.toString((char) re.getRunes()[0]).toLowerCase();
				}
				info.exact.add(exactStr);
				return info;
			}
			
			// convert all runes to lower case if not case sensitive
			if (!isCaseSensitive) {
				for (int i = 0; i < re.getRunes().length; i++) {
					re.getRunes()[i] = Character.toLowerCase(re.getRunes()[i]);
				}
			}
			
			int count = 0;
			for (int i = 0; i < re.getRunes().length; i += 2) {
				count += re.getRunes()[i+1] - re.getRunes()[i];
				// If the class is too large, it's okay to overestimate.
				if (count > 100) { 
					return RegexInfo.matchAny();
				}
				
				for (int codePoint = re.getRunes()[i]; codePoint <= re.getRunes()[i+1]; codePoint ++) {
					info.exact.add(Character.toString((char) codePoint));
				}
			}
			return info;
		case LITERAL:
			if (re.getRunes().length == 0) {
				return RegexInfo.emptyString();
			}
			String literal = "";
			if ((re.getFlags() & PublicRE2.FOLD_CASE) != PublicRE2.FOLD_CASE) {  // case sensitive
				for (int rune: re.getRunes()) {
					literal += Character.toString((char) rune);
				}
			} else {
				for (int rune: re.getRunes()) {
					literal += Character.toString((char) rune).toLowerCase();
				}
			}
			info = new RegexInfo();   
			info.exact.add(literal);
			info.simplify(false);
			return info;
		// A regex that indicates an expression is matched 
		// at least min times, at most max times.
		case REPEAT:
			// When min is zero, we treat REPEAT as START
			// When min is greater than zero, we treat REPEAT as PLUS, and let it fall through.
			if (re.getMin() == 0) {
				return RegexInfo.matchAny();
			}
			// !!!!! intentionally FALL THROUGH to PLUS !!!!!
		// A regex that indicates one or more occurrences of an expression.
		case PLUS:
			// The regexInfo of "(expr)+" should be the same as the info of "expr", 
			// except that "exact" is null, because we don't know the number of repetitions.
			info = analyze(re.getSubs()[0]);
			info.exact = null;
			return info;
		case QUEST:
			// The regexInfo of "(expr)?" shoud be either the same as the info of "expr",
			// or the same as the info of an empty string.
			return alternate(analyze(re.getSubs()[0]), RegexInfo.emptyString());
		// A regex that indicates zero or more occurrences of an expression.
		case STAR:
			return RegexInfo.matchAny();
		default:
			return RegexInfo.matchAny();
		}
	}
	
	/**
	 * This function calculates the {@code RegexInfo} of alternation of two given {@code RegexInfo}
	 * @param xInfo
	 * @param yInfo
	 * @return xyInfo
	 */
	private static RegexInfo alternate(RegexInfo xInfo, RegexInfo yInfo) {
		RegexInfo xyInfo = new RegexInfo();
		
		if (!xInfo.exact.isEmpty() && !yInfo.exact.isEmpty()) {
			xyInfo.exact = TranslatorUtils.union(xInfo.exact, yInfo.exact, false);
		} else if (!xInfo.exact.isEmpty()) {
			xyInfo.prefix = TranslatorUtils.union(xInfo.exact, yInfo.prefix, false);
			xyInfo.suffix = TranslatorUtils.union(xInfo.exact, yInfo.suffix, true);
			xInfo.addExactToMatch();
		} else if (!yInfo.exact.isEmpty()) {
			xyInfo.prefix = TranslatorUtils.union(xInfo.prefix, yInfo.exact, false);
			xyInfo.suffix = TranslatorUtils.union(xInfo.suffix, yInfo.exact, true);
			yInfo.addExactToMatch();
		} else {
			xyInfo.prefix = TranslatorUtils.union(xInfo.prefix, yInfo.prefix, false);
			xyInfo.suffix = TranslatorUtils.union(xInfo.suffix, yInfo.suffix, true);
		}
		
		xyInfo.emptyable = xInfo.emptyable || yInfo.emptyable;
		
		xyInfo.match = xInfo.match.or(yInfo.match);
		
		xyInfo.simplify(false);
		return xyInfo;
	}
	
	/**
	 * This function calculates the {@code RegexInfo} of concatenation of two given {@code RegexInfo}
	 * @param xInfo
	 * @param yInfo
	 * @return xyInfo
	 */
	private static RegexInfo concat(RegexInfo xInfo, RegexInfo yInfo) {
		RegexInfo xyInfo = new RegexInfo();
		
		xyInfo.match = xInfo.match.and(yInfo.match);
		
		if (!xInfo.exact.isEmpty() && !yInfo.exact.isEmpty()) {
			xyInfo.exact = TranslatorUtils.cartesianProduct(xInfo.exact, yInfo.exact, false);
		} else {
			if (!xInfo.exact.isEmpty()) {
				xyInfo.prefix = TranslatorUtils.cartesianProduct(xInfo.exact, yInfo.prefix, false);
			} else {
				xyInfo.prefix = xInfo.prefix;
				if (xInfo.emptyable) {
					xyInfo.prefix = TranslatorUtils.union(xyInfo.prefix, yInfo.prefix, false);
				}
			}
			
			if (!yInfo.exact.isEmpty()) {
				xyInfo.suffix = TranslatorUtils.cartesianProduct(xInfo.suffix, yInfo.exact, true);
			} else {
				xyInfo.suffix = yInfo.suffix;
				if (yInfo.emptyable) {
					xyInfo.suffix = TranslatorUtils.union(xyInfo.suffix, xInfo.suffix, true);
				}
			}
		}
		
		//TODO: customize 3
		if (xInfo.exact.isEmpty() && yInfo.exact.isEmpty() &&
				xInfo.suffix.size() <= TranslatorUtils.MAX_SET_SIZE && yInfo.prefix.size() <= TranslatorUtils.MAX_SET_SIZE &&
				TranslatorUtils.minLenOfString(xInfo.suffix) + TranslatorUtils.minLenOfString(yInfo.prefix) >= 3) {
			//TODO: is add the right function to use here??????
			xyInfo.match.add(TranslatorUtils.cartesianProduct(xInfo.suffix, yInfo.prefix, false));
		}
		
		xyInfo.simplify(false);
		
		return xyInfo;
	}
	
	private static RegexInfo fold (TranslatorUtils.IFold iFold, PublicRegexp[] subExpressions, RegexInfo zero) {
		if (subExpressions.length == 0) {
			return zero;
		} else if (subExpressions.length == 1) {
			return analyze(subExpressions[0]);
		}
		
		RegexInfo info = iFold.foldFunc(analyze(subExpressions[0]), analyze(subExpressions[1]));
		for (int i = 2; i < subExpressions.length; i++) {
			info = iFold.foldFunc(info, analyze(subExpressions[i]));
		}
		return info;
	}
	
}
