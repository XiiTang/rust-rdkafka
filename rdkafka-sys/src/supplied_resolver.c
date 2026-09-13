/* Native ABI shim only: placeholder address never used for external I/O. */
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <netdb.h>
#endif
#include <stddef.h>
struct rd_kafka_conf_s;
extern void rd_kafka_conf_set_resolve_cb(struct rd_kafka_conf_s *, int (*)(const char *, const char *, const struct addrinfo *, struct addrinfo **, void *));
static int supplied_resolve(const char *node, const char *service, const struct addrinfo *hints, struct addrinfo **res, void *opaque) {
    (void)opaque; (void)hints;
    if (!node && !service) { freeaddrinfo(*res); return 0; }
    struct addrinfo local = {0};
    local.ai_family = AF_INET; local.ai_socktype = SOCK_STREAM; local.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
    return getaddrinfo("127.0.0.1", "1", &local, res);
}
void rdkafka_install_supplied_resolver(struct rd_kafka_conf_s *conf) { rd_kafka_conf_set_resolve_cb(conf, supplied_resolve); }
