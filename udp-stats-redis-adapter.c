#include <sys/socket.h>
#include <netinet/in.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>

#include "credis.h"
#include "js0n.h"

struct msg_parts {
  char  *bucket;
  short  bucket_len;
  char  *kind;
  short  kind_len;
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
  parts->bucket     = NULL;
  parts->bucket_len = 0;
  parts->kind       = NULL;
  parts->kind_len   = 0;
  parts->timestamp  = NAN;
  parts->value      = NAN;
  unsigned short j[16] = {0};
  int i;
  if (js0n((void *)msg, n, j) == 0) {
    for (i = 0; i < 16; i += 4) {
      msg[j[i + 2] + j[i + 3]] = '\0';
      switch(msg[j[i]]) {
        case 'b': parts->bucket_len = j[i + 3]; parts->bucket = &msg[j[i + 2]]; break;
        case 'k': parts->kind_len   = j[i + 3]; parts->kind   = &msg[j[i + 2]]; break;
        case 't': parts->timestamp  = strtod(&msg[j[i + 2]], NULL); break;
        case 'v': parts->value      = strtod(&msg[j[i + 2]], NULL); break;
      }
    }
    return 0;
  }
  return -1;
}

int main (void) {

  int sock = init_udp_socket(INADDR_ANY, 6667);
  if (sock < 0) {
    printf("Error binding socket.\n");
    return 1;
  }
  printf("Now listening on UDP port %i...\n", 6667);

  // Declate Redis connection.
  REDIS red = NULL;

  // Declare msg buffer and parts.
  char msg[1024];
  struct msg_parts msg_parts;

  // Declare small helper values.
  int            n, t;
  struct timeval tv;

  // Run forever:
  while (1) {

    // Receive a message of length `n` < 1024 into buffer `msg`,
    // null-terminate the received message and determine current
    // timestamp.
    n      = recvfrom(sock, msg, 1023, 0, NULL, NULL);
    msg[n] = '\0';
    t      = gettimeofday(&tv, NULL);

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
      continue;
    }

    // If bucket is given, insert value into bucket.
    if (msg_parts.bucket) {
      char key[6 + msg_parts.bucket_len + 6];
      // If the received timestamp is wrong, and there
      // was an error calling gettimeofday(), we exit.
      // Otherwise we use the time returned by gettimeofday.
      if (isnan(msg_parts.timestamp)) {
        if (t == -1) {
          printf("Error retrieving timestamp. Skipping...\n");
          continue;
        }
        msg_parts.timestamp = (double)tv.tv_sec * 1000 + (double)tv.tv_usec;;
      }
      // Build the key string.
      memcpy  (&key[0], "stats:", 6);
      memmove (&key[6], msg_parts.bucket, msg_parts.bucket_len);
      // If kind is given, change kind of bucket.
      if (msg_parts.kind) {
        memcpy(&key[6 + msg_parts.bucket_len], ":kind", 6);
        credis_set(red, key, msg_parts.kind);
      }
      key[6 + msg_parts.bucket_len] = '\0';
      snprintf(msg, 1024, "%.0f:%f", msg_parts.timestamp, msg_parts.value);
      printf("%s\n", msg);
      credis_zadd(red, key, msg_parts.timestamp, msg);
    }

  }

  return 0;

}