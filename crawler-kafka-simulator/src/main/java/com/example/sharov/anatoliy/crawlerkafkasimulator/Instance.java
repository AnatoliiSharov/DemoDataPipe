package com.example.sharov.anatoliy.crawlerkafkasimulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.file.SyncableFileOutputStream;

public class Instance {
	
	public static final String INSTANCE = "Title_1|BodyOfNews_1|excample.site_1|teg1, teg2\nTitle_2|BodyOfNews_2|excample.site_1|teg1, teg2\n"
			+ "Title_3|BodyOfNews_3|excample.site_2|teg1, teg3\n"
			+ "Title_4|BodyOfNews_4|excample.site_1|teg1, teg4\n"
			+ "Title_5|BodyOfNews_5|excample.site_1|teg5, teg2";

	public List<ParsedNews> generate(String lineOfData){
		
		List<ParsedNews> results = new ArrayList();
		List<String> parts = Arrays.asList(lineOfData.split("\\\n"));
		
		for(String part : parts) {
			System.out.println(part);
			ParsedNews result = new ParsedNews();
			String[] fragments = part.split("\\|");
			result.setTitle(fragments[0]);
			System.out.println(result);
			result.setBody(fragments[1]);
			System.out.println(result);
			result.setLink(fragments[2]);
			System.out.println(result);
			result.setTags(Arrays.asList(fragments[3].split(", ")));
			System.out.println(result);
			results.add(result);
		}
		return results;
		
	}
	
}
