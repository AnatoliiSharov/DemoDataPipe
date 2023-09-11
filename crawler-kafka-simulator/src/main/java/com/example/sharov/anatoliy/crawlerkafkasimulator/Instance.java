package com.example.sharov.anatoliy.crawlerkafkasimulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Instance {
	
	public List<ParsedNews> generate(String lineOfData){
		List<ParsedNews> results = new ArrayList<ParsedNews>();
		List<String> parts = Arrays.asList(lineOfData.split("\\\n"));
		
		for(String part : parts) {
			ParsedNews result = new ParsedNews();
			String[] fragments = part.split("\\|");
			result.setTitle(fragments[0]);
			result.setBody(fragments[1]);
			result.setLink(fragments[2]);
			result.setTags(Arrays.asList(fragments[3].split(", ")));
			results.add(result);
		}
		return results;
		
	}
	
}
