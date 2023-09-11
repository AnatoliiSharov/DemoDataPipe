package com.example.sharov.anatoliy.flink;

import java.util.List;
import java.util.Objects;

public class ParsedNews {

	private Long id;
	private String title;
	private String body;
	private String link;
	private List<String> tags;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public String getLink() {
		return link;
	}
	public void setLink(String link) {
		this.link = link;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	@Override
	public int hashCode() {
		return Objects.hash(body, title);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ParsedNews other = (ParsedNews) obj;
		return Objects.equals(body, other.body) && Objects.equals(title, other.title);
	}
	@Override
	public String toString() {
		return "ParsedNews [id=" + id + ", title=" + title + ", body=" + body + ", link=" + link + ", tags=" + tags
				+ "]";
	}
	
}