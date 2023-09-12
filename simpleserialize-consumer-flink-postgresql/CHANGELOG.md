kafka-flink-postgresql-gradle-0.1-SNAPSHOT-all
it	catchs messages as String from kafka
	looks for existing this String in DB Postgresql 
		and either adding the number of repeated to this String or creating and adding new number for the new String
	saves (creating or updating)pares (String and Number) to DB
