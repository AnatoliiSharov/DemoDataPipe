DROP TABLE if EXISTS stories, tags, similar_stories, stories_tags, stories_similar_stories CASCADE;

CREATE TABLE IF NOT EXISTS stories(
id CHARACTER VARYING(100) NOT NULL UNIQUE, 
title CHARACTER VARYING(189819) NOT NULL,
url CHARACTER VARYING(189819) NOT NULL,
site CHARACTER VARYING(189819) NOT NULL,
time TIMESTAMP NOT NULL,
favicon_url CHARACTER VARYING(189819) NOT NULL,
description CHARACTER VARYING(189819)
);

CREATE TABLE IF NOT EXISTS tags(
id BIGSERIAL PRIMARY KEY NOT NULL,
tag CHARACTER VARYING(189819) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS similar_stories(
id BIGSERIAL PRIMARY KEY  NOT NULL,
similar_story CHARACTER VARYING(189819) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS stories_tags(
id BIGSERIAL PRIMARY KEY  NOT NULL,
story_id CHARACTER VARYING(100) REFERENCES stories(id) ON DELETE CASCADE  NOT NULL,
tag_id BIGINT REFERENCES tags(id) NOT NULL
);

CREATE TABLE IF NOT EXISTS stories_similar_stories(
id BIGSERIAL PRIMARY KEY NOT NULL,
story_id CHARACTER VARYING(100) REFERENCES stories(id) ON DELETE CASCADE  NOT NULL,
similar_story_id BIGINT REFERENCES similar_stories(id) NOT NULL
);

INSERT INTO stories (id, title, url, site, time, favicon_url, description) VALUES ('AAAAAAAAAAAA', 'title_A', 'url_A', 'site_A', '2023-11-14 12:08:08.965057', 'favicon_url_A', 'description_A');
INSERT INTO stories (id, title, url, site, time, favicon_url, description) VALUES ('CCCCCCCCCCCC', 'title_C', 'url_C', 'site_C', '2023-11-14 12:08:08.965057', 'favicon_url_C', 'description_C');

INSERT INTO tags (id, tag) VALUES ('10', 'existed_tag');
INSERT INTO tags (id, tag) VALUES ('11', 'existed_tag_too');

INSERT INTO similar_stories (id, similar_story) VALUES ('20', 'existed_similar_story');
INSERT INTO similar_stories (id, similar_story) VALUES ('21', 'existed_similar_story_too');

INSERT INTO stories_tags (story_id, tag_id) VALUES ('AAAAAAAAAAAA', '10');

INSERT INTO stories_similar_stories (story_id, similar_story_id) VALUES ('AAAAAAAAAAAA', '20');

