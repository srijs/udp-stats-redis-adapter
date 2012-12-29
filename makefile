build:
	$(CC) -Os -flto udp-stats-redis-adapter.c credis.c js0n.c -o udp-stats-redis-adapter
	strip udp-stats-redis-adapter