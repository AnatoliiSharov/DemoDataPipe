package com.example.sharov.anatoliy.flink;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import com.example.sharov.anatoliy.flink.protobuf.StoryProtos.Story;

public class StoryPojo implements Serializable{

	private static final long serialVersionUID = -3890635101385677878L;
	private String id;
	private String title;
	private String url;
	private String site; 
	private Long time; 
	private String favicon_url;
	private List<String> tags; 
	private List<String> similar_stories; 
	private String description;

	public StoryPojo parseFromMessageNews(Story message) {
		StoryPojo result = new StoryPojo();
		
		result.setId(message.getId());
		result.setTitle(message.getTitle());
		result.setUrl(message.getUrl());
		result.setSite(message.getSite());
		result.setTime(message.getTime());
		result.setFavicon_url(message.getFaviconUrl());
		result.setTags(message.getTagsList());
		result.setSimilar_stories(message.getSimilarStoriesList());
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

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getFavicon_url() {
		return favicon_url;
	}

	public void setFavicon_url(String favicon_url) {
		this.favicon_url = favicon_url;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public List<String> getSimilar_stories() {
		return similar_stories;
	}

	public void setSimilar_stories(List<String> similar_stories) {
		this.similar_stories = similar_stories;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
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
		StoryPojo other = (StoryPojo) obj;
		return Objects.equals(description, other.description) && Objects.equals(favicon_url, other.favicon_url)
				&& Objects.equals(id, other.id) && Objects.equals(similar_stories, other.similar_stories)
				&& Objects.equals(site, other.site) && Objects.equals(tags, other.tags)
				&& Objects.equals(time, other.time) && Objects.equals(title, other.title)
				&& Objects.equals(url, other.url);
	}

	@Override
	public String toString() {
		return "StoryPojo [id=" + id + ", title=" + title + ", url=" + url + ", site=" + site + ", time=" + time
				+ ", favicon_url=" + favicon_url + ", tags=" + tags + ", similar_stories=" + similar_stories
				+ ", description=" + description + "]";
	}

}
