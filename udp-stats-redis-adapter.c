#include <sys/socket.h>
#include <netinet/in.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "credis.h"
#include "js0n.h"

struct msg_parts {
  char  *bucket;
  char  *kind;
  double timestamp;
  double value;
};

static inline int init_udp_socket(long addr, short port) {
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  struct sockaddr_in server;
  memset(&server, 0, sizeof(server));
  server.sin_family      = AF_INET;
  server.sin_addr.s_addr = htonl(addr);
  server.sin_port        = htons(port);
  if (bind(fd, (struct sockaddr *)&server, sizeof(server)) == 0) {
    return fd;
  }
  return -1;
}

static inline int parse_msg_parts(struct msg_parts *parts, char *msg, int n) {
  unsigned short j[16] = {0};
  int i;
  if (js0n((void *)msg, n, j) == 0) {
    for (i = 0; i < 16; i += 4) {
      msg[j[i + 2] + j[i + 3]] = '\0';
      switch(msg[j[i]]) {
        case 'b': parts->bucket    = strdup(&msg[j[i + 2]]); break;
        case 'k': parts->kind      = strdup(&msg[j[i + 2]]); break;
        case 't': parts->timestamp = strtod(&msg[j[i + 2]], NULL); break;
        case 'v': parts->value     = strtod(&msg[j[i + 2]], NULL); break;
      }
    }
    return 0;
  }
  return -1;
}

static inline void free_msg_parts (struct msg_parts *parts) {
  free(parts->bucket);
  free(parts->kind);
}

int main (void) {

  int sock = init_udp_socket(INADDR_ANY, 6667);
  if (sock < 0) {
    printf("Error binding socket.\n");
    return 1;
  }
  printf("Now listening on UDP port %i...\n", 6667);

  // Declate Redis connection.
  REDIS red;

  // Declare msg buffer and parts.
  char msg[1024];
  struct msg_parts msg_parts = {0, 0, 0, 0};

  // Declare small helper values.
  int    n, m;
  time_t tm;

  // Run forever:
  while (1) {

    // Receive a message of length `n` < 1024 into buffer `msg`,
    // null-terminate the received message and determine current
    // timestamp.
    n      = recvfrom(sock, msg, 1023, 0, NULL, NULL);
    msg[n] = '\0';
    tm     = time(NULL);

    // Check for redis connection and try to establish, if
    // not existent yet.
    if (red == NULL) {
      if ((red = credis_connect(NULL, 6379, 2000)) == NULL) {
        printf("Error connecting to redis. Skipping...\n");
        continue;
      }
    }

    // Check for redis ping.
    if (credis_ping(red) < 0) {
      printf("Bad ping from redis server. Skipping...\n");
      red = NULL;
      continue;
    }

    // Check and parse JSON message into parts.
    if (parse_msg_parts(&msg_parts, msg, n) < 0) {
      printf("Malformed message. Skipping...\n");
      free_msg_parts(&msg_parts);
      continue;
    }

    // If bucket is given, insert value into bucket.
    if (msg_parts.bucket) {
      msg_parts.timestamp = msg_parts.timestamp < 1 ? (double)tm : msg_parts.timestamp;
      n = snprintf(&msg[0],   512, "stats:%s", msg_parts.bucket);
      m = snprintf(&msg[512], 512, "%.0f:%f",  msg_parts.timestamp, msg_parts.value);
      credis_zadd(red, &msg[0], msg_parts.timestamp, &msg[512]);
    }

    // If type is given, change type of bucket.
    if (msg_parts.bucket && msg_parts.kind) {
      n = snprintf(msg, 1024, "stats:%s:kind", msg_parts.bucket);
      credis_set(red, msg, msg_parts.kind);
    }

    free_msg_parts(&msg_parts);

  }

  return 0;

}