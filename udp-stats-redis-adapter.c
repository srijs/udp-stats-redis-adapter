#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include "js0n.h"

struct msg_parts {
  char *bucket,
       *kind,
       *timestamp,
       *value;
  short bucket_len,
        kind_len,
        timestamp_len,
        value_len;
};

static inline struct sockaddr_in sock_hint (long addr, short port) {
  struct sockaddr_in hints;
  memset(&hints, 0, sizeof(hints));
  hints.sin_family      = AF_INET;
  hints.sin_addr.s_addr = htonl(addr);
  hints.sin_port        = htons(port);
  return hints;
}

static inline int parse_msg (struct msg_parts *parts, char *msg, int n) {
  unsigned short j[16] = {0};
  int i;
  if (0 == js0n((void *)msg, n, j)) {
    for (i = 0; i < 16; i += 4) {
      msg[j[i + 2] + j[i + 3]] = '\0';
      switch(msg[j[i]]) {
        case 'b': parts->bucket_len    = j[i + 3]; parts->bucket    = &msg[j[i + 2]]; break;
        case 'k': parts->kind_len      = j[i + 3]; parts->kind      = &msg[j[i + 2]]; break;
        case 't': parts->timestamp_len = j[i + 3]; parts->timestamp = &msg[j[i + 2]]; break;
        case 'v': parts->value_len     = j[i + 3]; parts->value     = &msg[j[i + 2]]; break;
      }
    }
    return 0;
  }
  return -1;
}

int main (void) {

  // Ignore pipe signals.
  signal(SIGPIPE, SIG_IGN);

  // Initiate UDP server socket.
  int sock = socket(AF_INET, SOCK_DGRAM, 0);
  struct sockaddr_in server = sock_hint(INADDR_ANY, 6667);
  if (-1 == bind(sock, (struct sockaddr *)&server, sizeof(server))) {
    goto err_bind;
  }

  printf("Now listening on UDP port %i...\n", 6667);

  // Initiate redis client socket.
  int red = -1;

  // Declare msg, send buffer and parts.
  char msg[1024], send[1024];
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
    if (-1 == red) {
      red = socket(AF_INET, SOCK_STREAM, 0);
      struct sockaddr_in client = sock_hint(inet_addr("1.0.0.127"), 6379);
      if (-1 == connect(red, (struct sockaddr *)&client, sizeof(client))) {
        goto err_redis_connection;
      }
    }

    // Check and parse JSON message into parts.
    msg_parts = (struct msg_parts){NULL, NULL, NULL, NULL, 0, 0, 0, 0};
    if (-1 == parse_msg(&msg_parts, msg, n)) {
      goto err_json;
    }

    // If bucket is given, insert value into bucket.
    if (msg_parts.bucket) {

      // If the received timestamp is wrong, and there
      // was an error calling gettimeofday(), we exit.
      // Otherwise we use the time returned by gettimeofday.
      if (msg_parts.timestamp == NULL) {
        if (-1 == t) {
          goto err_timestamp;
        }
        msg_parts.timestamp = &msg[n + 1];
        msg_parts.timestamp_len = snprintf(msg_parts.timestamp, 1024 - n - 1, "%.f", (double)tv.tv_sec * 1000 + (double)tv.tv_usec);
      }

      // Build and send redis query.
      if (-1 == write(red, send, snprintf(send, 1024, "ZADD stats:%s %s %i\r\n%s:%s\r\n",
                                          msg_parts.bucket, msg_parts.timestamp,
                                          msg_parts.timestamp_len + 1 + msg_parts.value_len,
                                          msg_parts.timestamp, msg_parts.value))) {
        goto err_redis_write;
      }

      // If kind is given, also change kind of bucket.
      if (msg_parts.kind) {
        if (-1 == write(red, send, snprintf(send, 1024, "SET stats:%s:kind %i\r\n%s\r\n",
                                            msg_parts.bucket, msg_parts.kind_len,
                                            msg_parts.kind))) {
          goto err_redis_write;
        }
      }

    }

    continue;

    err_bind:             printf("Error binding socket.       Exiting...\n");
                          return 1;

    err_redis_connection: printf("Error connecting to redis.  Skipping...\n");
                          red = -1;
                          continue;

    err_redis_write:      printf("Error writing to redis.     Skipping...\n");
                          red = -1;
                          continue;

    err_timestamp:        printf("Error retrieving timestamp. Skipping...\n");
                          continue;

    err_json:             printf("Error parsing json.         Skipping...\n");
                          continue;

  }

}