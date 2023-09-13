package com.example.sharov.anatoliy.simpleserialize.produser;

import java.util.List;
import java.util.Objects;

public class ParsedNews {

	private String title;
	private String body;
	private String link;
	
	@Override
	//TODO temporary custom method toString
	public String toString() {
		return "ParsedNews [" + title + ", " + body + ", " + link + "]";
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

	@Override
	public int hashCode() {
		return Objects.hash(body, link, title);
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
		return Objects.equals(body, other.body) && Objects.equals(link, other.link)
				&& Objects.equals(title, other.title);
	}
	
}
