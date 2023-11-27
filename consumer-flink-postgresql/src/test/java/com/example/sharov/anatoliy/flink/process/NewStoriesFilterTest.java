package com.example.sharov.anatoliy.flink.process;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.sharov.anatoliy.flink.entity.StoryPojo;
import com.example.sharov.anatoliy.flink.service.Serv;

@ExtendWith(MockitoExtension.class)
class NewStoriesFilterTest {
	private StoryPojo value;
	@Mock
	private Serv service;
	@InjectMocks
	private NewStoriesFilter filter;
	
	@BeforeEach
	void setUp() throws Exception {
		value = new StoryPojo.Builder()
				.url("test")
				.description("test")
				.faviconUrl("test")
				.site("test")
				.time(Timestamp.valueOf("2023-11-14 12:08:08.965057"))
				.title("test")
				.build();
	}
	
	@Test
	void testFilter_shouldFalse_whenIdExists() throws Exception {
		String id = "existedId";
		value.setId(id);
		when(service.checkStoryAlreadyExist(value)).thenReturn(false);
		
		assertFalse(filter.filter(value));
		
		verify(service, times(1)).checkStoryAlreadyExist(any());
	}
	
	@Test
	void testFilter_shouldTrue_whenNewId() throws Exception {
		String id = "newId";
		value.setId(id);
		when(service.checkStoryAlreadyExist(value)).thenReturn(true);
		
		assertTrue(filter.filter(value));
		
		verify(service).checkStoryAlreadyExist(any());
	}

}
