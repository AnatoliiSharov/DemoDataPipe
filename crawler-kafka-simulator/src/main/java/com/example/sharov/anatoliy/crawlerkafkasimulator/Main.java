package com.example.sharov.anatoliy.crawlerkafkasimulator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.jexl3.JexlContext;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.tools.FileDumper;
import org.apache.nutch.util.NutchConfiguration;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public static final String INPUT_TOPIC = "mytopic";
	public static final String BOOTSTAP_SERVERS = "broker:29092";
	public static final String ASK = "all";
	
	static final int MAX_URLS_PER_SEGMENT = 1;
	static final int MAX_CONCURRENT_REQUESTS = 10;
	static final long MINIMUM_INTERVAL = 1000L;

	public static final String PLUGINS_DIR = "/home/anatolii/opt/apache-nutch-1.19/plugins";
	
	private Main() {
    }
	
    public static void main(final String... args) throws Exception {
        final Configuration conf = NutchConfiguration.create();
        
        conf.set("http.agent.name", "I'm a test crawler");
        conf.set("plugin.folders", "/home/anatolii/opt/apache-nutch-1.19/plugins");
//        conf.set("file.content.ignored", "false");
        final Path home = new Path("/home/anatolii/opt/apache-nutch-1.19");
        FileUtils.forceMkdir(new File(home.toString()));
        final Path targets = new Path(home, "urls");
        FileUtils.forceMkdir(new File(targets.toString()));
        final String[] urls = {"https://ru.wikipedia.org/wiki/Nutch"};
        Files.write(
            Paths.get(targets.toString(), "seed.txt"),
            String.join("\n", urls).getBytes()
        );
        new Injector(conf).inject(
            new Path(home, "crawldb"),
            new Path(home, "urls"),
            true, true
        );
        for (int idx = 0; idx < 2; ++idx) {
            Main.cycle(home, conf);
        }
        FileUtils.forceMkdir(new File(new Path(home, "dump").toString()));
        new FileDumper().dump(
            new File(new Path(home, "dump").toString()),
            new File(new Path(home, "segments").toString()),
            null, true, false, false
        );
    }

    private static void cycle(final Path home,
        final Configuration conf) throws Exception {
        final Path segments = new Path(home, "segments");
        new Generator(conf).generate(
            new Path(home, "crawldb"),
            new Path(home, "segments"),
            // @checkstyle MagicNumber (1 line)
            1, 1000L, System.currentTimeMillis()
        );
        final Path sgmt = Main.segment(segments);
        new Fetcher(conf).fetch(
            // @checkstyle MagicNumber (1 line)
            sgmt, 10
        );
        new ParseSegment(conf).parse(sgmt);
        new CrawlDb(conf).update(
            new Path(home, "crawldb"),
            Files.list(Paths.get(segments.toString()))
                .map(p -> new Path(p.toString()))
                .toArray(Path[]::new),
            true, true
        );
    }

    private static Path segment(final Path dir) throws IOException {
        final List<Path> list = Files.list(Paths.get(dir.toString()))
            .map(p -> new Path(p.toString()))
            .sorted(Comparator.comparing(Path::toString))
            .collect(Collectors.toList());
        return list.get(list.size() - 1);
    }
}