package com.example.sharov.anatoliy.crawlerkafkasimulator;

import java.net.URL;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class ParsedNews {

	private String title;
	private String body;
	private String link;
	private List<String> tegs;
	
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
	public List<String> getTegs() {
		return tegs;
	}
	public void setTegs(List<String> tegs) {
		this.tegs = tegs;
	}
	@Override
	public int hashCode() {
		return Objects.hash(body, link, tegs, title);
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
		return Objects.equals(body, other.body) && Objects.equals(link, other.link) && Objects.equals(tegs, other.tegs)
				&& Objects.equals(title, other.title);
	}
	@Override
	public String toString() {
		return "ParsedNews [title=" + title + ", body=" + body + ", link=" + link + ", tegs=" + tegs + "]";
	}
	
}
