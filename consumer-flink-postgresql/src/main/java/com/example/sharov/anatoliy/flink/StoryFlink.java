package com.example.sharov.anatoliy.flink;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple2;

import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;

public class StoryFlink {
	private static final Long UNABLE_ID_DATA_EXISTS = 0L; 
	private static final Long UNABLE_ID_MISSING_DATA = -1L; 

	private String id;
	private String title;
	private String url;
	private String site;
	private Timestamp time;
	private String favicon_url;
	private List<Tuple2<Long, String>> tags;
	private List<Tuple2<Long, String>> similar_stories;
	private String description;

	public StoryFlink parseFromMessageNews(Story message) {
		StoryFlink result = new StoryFlink();

		result.setId(message.getId());
		result.setTitle(message.getTitle());
		result.setUrl(message.getUrl());
		result.setSite(message.getSite());
		result.setTime(new Timestamp(Long.valueOf(message.getTime())));
		result.setFavicon_url(message.getFaviconUrl());
        result.setTags(message.getTagsList().stream().map(each -> {
        	Tuple2<Long, String> instedOfTag = new Tuple2<>(UNABLE_ID_MISSING_DATA, null);
        	
        		if(each!=null) {
        			instedOfTag = Tuple2.of(UNABLE_ID_DATA_EXISTS, each);
        		} 
        		return instedOfTag;
        	}).collect(Collectors.toList()));
		result.setSimilar_stories(message.getSimilarStoriesList().stream().map(each -> {
			Tuple2<Long, String> instedOfSimilarStory = new Tuple2<>(UNABLE_ID_DATA_EXISTS, null);
        	
        		if(each!=null) {
        			instedOfSimilarStory = Tuple2.of(UNABLE_ID_DATA_EXISTS, each);
        		} 
        		return instedOfSimilarStory;
			}).collect(Collectors.toList()));
		result.setDescription(message.getDescription());
		return result;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getSite() {
		return site;
	}

	public void setSite(String site) {
		this.site = site;
	}

	public Timestamp getTime() {
		return time;
	}

	public void setTime(Timestamp time) {
		this.time = time;
	}

	public String getFavicon_url() {
		return favicon_url;
	}

	public void setFavicon_url(String favicon_url) {
		this.favicon_url = favicon_url;
	}

	public List<Tuple2<Long, String>> getTags() {
		return tags;
	}

	public void setTags(List<Tuple2<Long, String>> tags) {
		this.tags = tags;
	}

	public List<Tuple2<Long, String>> getSimilar_stories() {
		return similar_stories;
	}

	public void setSimilar_stories(List<Tuple2<Long, String>> similar_stories) {
		this.similar_stories = similar_stories;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public int hashCode() {
		return Objects.hash(description, favicon_url, id, similar_stories, site, tags, time, title, url);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StoryFlink other = (StoryFlink) obj;
		return Objects.equals(description, other.description) && Objects.equals(favicon_url, other.favicon_url)
				&& Objects.equals(id, other.id) && Objects.equals(similar_stories, other.similar_stories)
				&& Objects.equals(site, other.site) && Objects.equals(tags, other.tags)
				&& Objects.equals(time, other.time) && Objects.equals(title, other.title)
				&& Objects.equals(url, other.url);
	}

	@Override
	public String toString() {
		return "StoryFlink [id=" + id + ", title=" + title + ", url=" + url + ", site=" + site + ", time=" + time
				+ ", favicon_url=" + favicon_url + ", tags=" + tags + ", similar_stories=" + similar_stories
				+ ", description=" + description + "]";
	}

}
