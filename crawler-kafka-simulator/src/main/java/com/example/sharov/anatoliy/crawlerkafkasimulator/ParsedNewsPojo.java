package com.example.sharov.anatoliy.crawlerkafkasimulator;

import java.net.URL;
import java.time.LocalDate;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ParsedNewsPojo {

	private String newsTitle;
	private String newsBody;
	private String newsLink;
	private String siteLink;
	private List<String> newsPresentTegs;
	private LocalDate newsDate;
	
}
