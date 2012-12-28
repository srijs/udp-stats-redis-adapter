build:
	$(CC) -Os -flto udp-stats-redis-adapter.c credis.c parson.c -o udp-stats-redis-adapter
	strip udp-stats-redis-adapter