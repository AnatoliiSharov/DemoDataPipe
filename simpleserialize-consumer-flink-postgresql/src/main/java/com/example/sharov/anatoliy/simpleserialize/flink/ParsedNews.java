package com.example.sharov.anatoliy.simpleserialize.flink;

import java.io.Serializable;
import java.util.Objects;

public class ParsedNews implements Serializable{

	private static final long serialVersionUID = -3890635101385677878L;
	private Long id;
	private String title;
	private String body;
	private String link;
	private String tag;

	@Override
	public String toString() {
		return "ParsedNews [id=" + id + ", title=" + title + ", body=" + body + ", link=" + link + ", tag=" + tag
				+ "]";
	}

	//TODO temporary kill it please after 
	public static ParsedNews parseFromString(String value) {
		ParsedNews parsedNews = new ParsedNews();
		String v = value.substring("ParsedNews [".length(), value.length() - 1);
		parsedNews.setId(1L);
		parsedNews.setTitle(v.split(", ")[0]);
		parsedNews.setBody(v.split(", ")[1]);
		parsedNews.setLink(v.split(", ")[2]);
		return parsedNews;
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

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	@Override
	public int hashCode() {
		return Objects.hash(body, id, link, tag, title);
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
				&& Objects.equals(tag, other.tag) && Objects.equals(title, other.title);
	}
	
}