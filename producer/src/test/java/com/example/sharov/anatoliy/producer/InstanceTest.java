package com.example.sharov.anatoliy.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.*;

class InstanceTest {

	private Instance instance;
	
	
	@Test
	void test() {
		instance = new Instance();
		String lineOfData = "id_1|title_1|url_1|site_1|1111111111|favicon_url_1|teg1, teg2|similar_stories_1, similar_stories_2|description1\n";
		String expected = "[StoryPojo [id=id_1, title=title_1, url=url_1, site=site_1, time=1111111111, favicon_url=favicon_url_1, tags=[teg1, teg2], similar_stories=[similar_stories_1, similar_stories_2], description=description1]]";
		
		assertEquals(expected, instance.generate(lineOfData).toString());
	
	}

}
