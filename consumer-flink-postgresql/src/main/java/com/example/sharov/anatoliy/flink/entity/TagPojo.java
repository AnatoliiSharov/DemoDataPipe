package com.example.sharov.anatoliy.flink.entity;

import java.io.Serializable;
import java.util.Objects;

public class TagPojo implements Serializable{
	private static final long serialVersionUID = 6745814525822049131L;
	
	private Long id;
	private String tag;
	
	public TagPojo() {
		super();
	}
	
	public TagPojo(Long id, String tag) {
		super();
		this.id = id;
		this.tag = tag;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, tag);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TagPojo other = (TagPojo) obj;
		return Objects.equals(id, other.id) && Objects.equals(tag, other.tag);
	}

	@Override
	public String toString() {
		return "Tag [id=" + id + ", tag=" + tag + "]";
	}

}
