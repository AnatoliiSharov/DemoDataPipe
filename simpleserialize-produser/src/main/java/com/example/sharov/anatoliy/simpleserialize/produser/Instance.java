package com.example.sharov.anatoliy.simpleserialize.produser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Instance {
	public final String PROJECT_MARK = "string-data-";
	
	public List<ParsedNews> generate(String lineOfData){
		List<ParsedNews> results = new ArrayList<ParsedNews>();
		List<String> parts = Arrays.asList(lineOfData.split("\\\n"));
		
		for(String part : parts) {
			ParsedNews result = new ParsedNews();
			String[] fragments = part.split("\\|");
			result.setTitle(PROJECT_MARK+fragments[0]);
			result.setBody(PROJECT_MARK+fragments[1]);
			result.setLink(PROJECT_MARK+fragments[2]);
//			result.setTags(Stream.of(fragments[3].split(", ")).map(e -> PROJECT_MARK + e).collect(Collectors.toList()));
			results.add(result);
		}
		return results;
	}
	
}
