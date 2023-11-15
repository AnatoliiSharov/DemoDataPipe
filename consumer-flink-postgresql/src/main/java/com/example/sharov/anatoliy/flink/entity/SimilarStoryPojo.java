package com.example.sharov.anatoliy.flink.entity;

import java.util.Objects;

public class SimilarStoryPojo {

	private Long id;
	private String similarStory;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getSimilarStory() {
		return similarStory;
	}

	public void setSimilarStory(String similarStory) {
		this.similarStory = similarStory;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, similarStory);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SimilarStoryPojo other = (SimilarStoryPojo) obj;
		return Objects.equals(id, other.id) && Objects.equals(similarStory, other.similarStory);
	}

	@Override
	public String toString() {
		return "SimilarStory [id=" + id + ", similarStory=" + similarStory + "]";
	}

}
