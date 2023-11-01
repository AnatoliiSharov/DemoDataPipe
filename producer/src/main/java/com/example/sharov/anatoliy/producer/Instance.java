package com.example.sharov.anatoliy.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Instance {

	public List<StoryPojo> generate(String lineOfData){
		List<StoryPojo> results = new ArrayList<StoryPojo>();
		List<String> parts = Arrays.asList(lineOfData.split("\\\n"));
		
		for(String part : parts) {
			StoryPojo result = new StoryPojo();
			String[] fragments = part.split("\\|");
			result.setId(fragments[0]);
			result.setTitle(fragments[1]);
			result.setUrl(fragments[2]);
			result.setSite(fragments[3]);
			result.setTime(Long.valueOf(fragments[4]));
			result.setFavicon_url(fragments[5]);
			result.setTags(Stream.of(fragments[6].split(", ")).collect(Collectors.toList()));
			result.setSimilar_stories(Stream.of(fragments[7].split(", ")).collect(Collectors.toList()));
			results.add(result);
		}
		return results;
	}
	
}
