package com.example.sharov.anatoliy.flink;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import com.example.sharov.anatoliy.flink.protobuf.NewsProtos.News;

public class ParsedNews implements Serializable{

	private static final long serialVersionUID = -3890635101385677878L;
	private Long id;
	private String title;
	private String body;
	private String link;
	private List<String> tags;

	public ParsedNews parseFromMessageNews(News message) {
		ParsedNews result = new ParsedNews();
		
		result.setTitle(message.getTitle());
		result.setBody(message.getBody());
		result.setLink(message.getLink());
		result.setTags(message.getTagsList());
		return result;
	}

	@Override
	public int hashCode() {
		return Objects.hash(body, id, link, tags, title);
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
		return Objects.equals(body, other.body) && Objects.equals(id, other.id) && Objects.equals(link, other.link)
				&& Objects.equals(tags, other.tags) && Objects.equals(title, other.title);
	}

	@Override
	public String toString() {
		return "ParsedNews [id=" + id + ", title=" + title + ", body=" + body + ", link=" + link + ", tags=" + tags
				+ "]";
	}

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
	
}
