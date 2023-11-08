package com.example.sharov.anatoliy.flink.conf;

public class StoryUtil {
	
	public String[] mapToRowForStoryTable(StoryFlink story) {
		return new String[] {
				story.getId(),
				story.getTitle(),
				story.getUrl(),
				story.getSite(),
				story.getTime().toString(),
				story.getFavicon_url(),
				story.getDescription(),
		};
	}
	
}
