package com.example.sharov.anatoliy;

import java.util.Objects;

public class CountedWordPojo {
	private String word;
	private int number;

	public CountedWordPojo(String word, int number) {
		super();
		this.word = word;
		this.number = number;
	}

	public CountedWordPojo(String word) {
		super();
		this.word = word;
		this.number = 0;
	}

	public CountedWordPojo() {
		super();
		this.word = null;
		this.number = 0;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}

	@Override
	public int hashCode() {
		return Objects.hash(number, word);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CountedWordPojo other = (CountedWordPojo) obj;
		return number == other.number && Objects.equals(word, other.word);
	}

	@Override
	public String toString() {
		return "CountedWordPojo [word=" + word + ", number=" + number + "]";
	}

}
