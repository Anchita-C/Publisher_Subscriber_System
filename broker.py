import socket
import threading
import json
import ssl
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("broker")

HOST = '0.0.0.0'
PORT = 9010
HEADER_SIZE = 10
FORMAT = "utf-8"

CERTFILE = "server.crt"
KEYFILE  = "server.key"

topics = {}
lock = threading.Lock()

perf = {
    "messages_published": 0,
    "messages_delivered": 0,
    "clients_connected":  0,
    "start_time":         time.time(),
}
perf_lock = threading.Lock()

def receive_json(conn, first_byte=None):
    try:
        if first_byte:
            header = first_byte + conn.recv(HEADER_SIZE - 1)
        else:
            header = conn.recv(HEADER_SIZE)
        if not header:
            return None
        try:
            msg_length = int(header.decode(FORMAT).strip())
        except ValueError:
            log.warning("Malformed header — dropping message")
            return None

        if msg_length <= 0 or msg_length > 1_000_000:
            log.warning(f"Suspicious message length {msg_length} — dropping")
            return None

        data = b""
        while len(data) < msg_length:
            packet = conn.recv(msg_length - len(data))
            if not packet:
                return None
            data += packet

        return json.loads(data.decode(FORMAT))

    except json.JSONDecodeError:
        log.warning("Invalid JSON payload — dropping message")
        return None
    except ssl.SSLError as e:
        log.error(f"SSL error during read: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error in receive_json: {e}")
        return None

def handle_client(conn, addr):
    with perf_lock:
        perf["clients_connected"] += 1

    log.info(f"[CONNECTED] {addr}  (active clients: {perf['clients_connected']})")

    while True:
        try:
            first_byte = conn.recv(1)
            if not first_byte:
                break

            if first_byte in b"0123456789 {":
                msg = receive_json(conn, first_byte)
                if msg is None:
                    break

                command = msg.get("cmd", "").upper()

                if command == "CREATE":
                    topic = msg.get("topic", "").strip()
                    if not topic:
                        log.warning("[CREATE] Empty topic name — ignored")
                        continue
                    with lock:
                        if topic not in topics:
                            topics[topic] = []
                            log.info(f"[CREATE] New topic: '{topic}'")
                        else:
                            log.info(f"[CREATE] Topic '{topic}' already exists")

                elif command == "PUBLISH":
                    topic   = msg.get("topic", "").strip()
                    data    = msg.get("data", "")
                    sent_at = msg.get("timestamp", time.time())

                    if not topic:
                        log.warning("[PUBLISH] Empty topic name — ignored")
                        continue

                    latency_ms = (time.time() - sent_at) * 1000

                    with perf_lock:
                        perf["messages_published"] += 1

                    with lock:
                        if topic not in topics:
                            topics[topic] = []
                        subscribers = list(topics[topic])

                    log.info(f"[PUBLISH] '{topic}' | {len(subscribers)} subscribers | latency {latency_ms:.2f} ms")

                    delivered = 0
                    dead_subs = []

                    for sub in subscribers:
                        try:
                            sub.send(f"{topic}:{data}\n".encode(FORMAT))
                            delivered += 1
                        except Exception as e:
                            log.warning(f"[DELIVER] Failed → marking dead: {e}")
                            dead_subs.append(sub)

                    if dead_subs:
                        with lock:
                            for dead in dead_subs:
                                if dead in topics.get(topic, []):
                                    topics[topic].remove(dead)

                    with perf_lock:
                        perf["messages_delivered"] += delivered

                    log.info(f"[ROUTED] '{topic}' → {delivered}/{len(subscribers)} delivered")

                else:
                    log.warning(f"[UNKNOWN CMD] {command} — ignored")

            else:
                raw = (first_byte + conn.recv(1024)).decode(FORMAT)
                if not raw:
                    break

                for line in raw.strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue

                    parts   = line.split(" ", 2)
                    command = parts[0].upper()

                    if command == "SUBSCRIBE":
                        if len(parts) < 2:
                            log.warning("[SUBSCRIBE] Missing topic name")
                            continue
                        topic = parts[1].strip()
                        if not topic:
                            continue
                        with lock:
                            if topic not in topics:
                                topics[topic] = []
                            if conn not in topics[topic]:
                                topics[topic].append(conn)
                        log.info(f"[SUBSCRIBE] {addr} → '{topic}'")

                    elif command == "UNSUBSCRIBE":
                        if len(parts) < 2:
                            log.warning("[UNSUBSCRIBE] Missing topic name")
                            continue
                        topic = parts[1].strip()
                        with lock:
                            if topic in topics and conn in topics[topic]:
                                topics[topic].remove(conn)
                        log.info(f"[UNSUBSCRIBE] {addr} ✗ '{topic}'")

                    elif command == "LIST_TOPICS":
                        with lock:
                            topic_list = list(topics.keys())
                        response = "TOPICS:" + ",".join(topic_list) + "\n"
                        try:
                            conn.send(response.encode(FORMAT))
                        except Exception as e:
                            log.warning(f"[LIST_TOPICS] Send failed: {e}")
                        log.info(f"[LIST_TOPICS] {len(topic_list)} topics → {addr}")

                    elif command == "STATS":
                        with perf_lock:
                            uptime  = time.time() - perf["start_time"]
                            pub     = perf["messages_published"]
                            deliv   = perf["messages_delivered"]
                            clients = perf["clients_connected"]
                        throughput = pub / uptime if uptime > 0 else 0
                        stats_msg = (
                            f"STATS:"
                            f"uptime={uptime:.1f}s,"
                            f"published={pub},"
                            f"delivered={deliv},"
                            f"clients={clients},"
                            f"throughput={throughput:.2f}msg/s\n"
                        )
                        try:
                            conn.send(stats_msg.encode(FORMAT))
                        except Exception as e:
                            log.warning(f"[STATS] Send failed: {e}")

                    else:
                        log.warning(f"[UNKNOWN CMD] '{command}' from {addr}")

        except ssl.SSLError as e:
            log.error(f"[SSL ERROR] {addr}: {e}")
            break
        except ConnectionResetError:
            log.warning(f"[RESET] {addr} forcibly closed the connection")
            break
        except Exception as e:
            log.error(f"[ERROR] Unexpected in handle_client for {addr}: {e}")
            break

    with lock:
        for topic in topics:
            if conn in topics[topic]:
                topics[topic].remove(conn)

    with perf_lock:
        perf["clients_connected"] = max(0, perf["clients_connected"] - 1)

    try:
        conn.close()
    except Exception:
        pass

    log.info(f"[DISCONNECTED] {addr}  (active clients: {perf['clients_connected']})")

def build_ssl_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    try:
        context.load_cert_chain(certfile=CERTFILE, keyfile=KEYFILE)
    except FileNotFoundError:
        log.critical(
            f"Certificate or key file not found ({CERTFILE} / {KEYFILE}).\n"
            "Generate them with:\n"
            "  openssl req -x509 -newkey rsa:2048 -keyout server.key "
            "-out server.crt -days 365 -nodes -subj '/CN=localhost'"
        )
        raise

    return context

def start_broker():
    ssl_context = build_ssl_context()

    raw_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    raw_server.bind((HOST, PORT))
    raw_server.listen(50)

    server = ssl_context.wrap_socket(raw_server, server_side=True)

    log.info(f"[BROKER RUNNING] SSL/TLS on {HOST}:{PORT}")
    log.info(f"[CERT] {CERTFILE}  |  [KEY] {KEYFILE}")

    while True:
        try:
            conn, addr = server.accept()
        except ssl.SSLError as e:
            log.warning(f"[SSL HANDSHAKE FAILED] {e}")
            continue
        except KeyboardInterrupt:
            log.info("[SHUTDOWN] Broker stopped by user")
            break
        except Exception as e:
            log.error(f"[ACCEPT ERROR] {e}")
            continue

        thread = threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True
        )
        thread.start()
        log.info(f"[THREAD] Spawned for {addr}  (total: {threading.active_count()} threads)")

if __name__ == "__main__":
    start_broker()