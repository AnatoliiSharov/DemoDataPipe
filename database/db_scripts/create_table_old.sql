
CREATE TABLE IF NOT EXISTS newses(
id BIGSERIAL PRIMARY KEY  NOT NULL,
title CHARACTER VARYING(189819) NOT NULL,
text TEXT NOT NULL,
link CHARACTER VARYING(189819) NOT NULL,
hash_news INT NOT NULL
);

CREATE TABLE IF NOT EXISTS tags(
id BIGSERIAL PRIMARY KEY  NOT NULL,
id_news BIGINT REFERENCES newses(id) ON DELETE CASCADE,
tag CHARACTER VARYING(189819) NOT NULL
);

Story field	Description
id	A unique string id of the story. The id can be used for pagination as the value of the parameter last.
title	The title of the news story.
url	The url of the news story.
site	The source website of the news story.
time	The timestamp of the news story. It's the number of milliseconds since the "Unix epoch", 1970-01-01T00:00:00Z (UTC). The same semantics as Date.now() in Javascript.
favicon_url	The url of the favicon of the source website.
tags	An array of strings. Each string is the ticker for which the story is. This field is presented only when any tt: term is in the query.
similar_stories	An array of strings. Each string is a story ID referencing another story in the response. The referenced stories are considered stories similar to this one. This field is optional.
description	The description of the news story. This field is optional.

