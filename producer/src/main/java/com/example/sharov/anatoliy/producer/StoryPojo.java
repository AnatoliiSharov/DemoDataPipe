package com.example.sharov.anatoliy.producer;

import java.util.List;
import java.util.Objects;

public class StoryPojo {

	String id;
	String title;
	String url;
	String site;
	String time;
	String favicon_url;
	List<String> tags;
	List<String> similar_stories;
	String description;

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

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
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
