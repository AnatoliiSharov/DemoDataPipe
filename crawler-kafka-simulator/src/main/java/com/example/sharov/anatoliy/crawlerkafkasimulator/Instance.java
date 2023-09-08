package com.example.sharov.anatoliy.crawlerkafkasimulator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Instance {

	public List<ParsedNews> generate(String lineOfData){
		return Arrays.stream(lineOfData.split("\\n"))
				.map(each -> {
			
					ParsedNews news = new ParsedNews();
					String[] p = each.split("|");
					news.setTitle(p[0]);
					news.setBody(p[1]);
					news.setLink(p[2]);
					news.setTags(Arrays.asList(p[3].split(", ")));
					return news;})
				.collect(Collectors.toList());
	}
	
}
