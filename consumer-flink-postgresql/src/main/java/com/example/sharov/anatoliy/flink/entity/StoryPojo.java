package com.example.sharov.anatoliy.flink.entity;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.flink.api.java.tuple.Tuple3;

public class StoryPojo {
	private static final long serialVersionUID = 1L;

	private String id;
	private String title;
	private String url;
	private String site;
	private Timestamp time;
	private String faviconUrl;
	private List<TagPojo> tags;
	private List<SimilarStoryPojo> similarStories;
	private String description;

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

	public String getFaviconUrl() {
		return faviconUrl;
	}

	public void setFaviconUrl(String faviconUrl) {
		this.faviconUrl = faviconUrl;
	}

	public List<TagPojo> getTags() {
		return tags;
	}

	public void setTags(List<TagPojo> tags) {
		this.tags = tags;
	}

	public List<SimilarStoryPojo> getSimilarStories() {
		return similarStories;
	}

	public void setSimilarStories(List<SimilarStoryPojo> similarStories) {
		this.similarStories = similarStories;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public int hashCode() {
		return Objects.hash(description, faviconUrl, id, similarStories, site, tags, time, title, url);
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
		return Objects.equals(description, other.description) && Objects.equals(faviconUrl, other.faviconUrl)
				&& Objects.equals(id, other.id) && Objects.equals(similarStories, other.similarStories)
				&& Objects.equals(site, other.site) && Objects.equals(tags, other.tags)
				&& Objects.equals(time, other.time) && Objects.equals(title, other.title)
				&& Objects.equals(url, other.url);
	}

	@Override
	public String toString() {
		return "StoryPojo [id=" + id + ", title=" + title + ", url=" + url + ", site=" + site + ", time=" + time
				+ ", faviconUrl=" + faviconUrl + ", tags=" + tags + ", similarStories=" + similarStories
				+ ", description=" + description + "]";
	}

	public static class Builder {
		private StoryPojo story = new StoryPojo();

		public Builder id(String id) {
			story.id = id;
			return this;
		}

		public Builder title(String title) {
			story.title = title;
			return this;
		}

		public Builder url(String url) {
			story.url = url;
			return this;
		}

		public Builder site(String site) {
			story.site = site;
			return this;
		}

		public Builder time(Timestamp time) {
			story.time = time;
			return this;
		}

		public Builder faviconUrl(String faviconUrl) {
			story.faviconUrl = faviconUrl;
			return this;
		}

		public Builder tags(List<TagPojo> tags) {
			story.tags = tags;
			return this;
		}

		public Builder addTag(TagPojo tag) {
			story.tags.add(tag);
			return this;
		}

		public Builder similarStories(List<SimilarStoryPojo> similarStories) {
			story.similarStories = similarStories;
			return this;
		}

		public Builder addSimilarStoriy(SimilarStoryPojo similarStory) {
			story.similarStories.add(similarStory);
			return this;
		}

		public Builder description(String description) {
			story.description = description;
			return this;
		}

		public StoryPojo build() {
			return story;
		}
	}

}
