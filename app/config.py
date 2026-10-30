import os

NATS_URL = os.getenv("NATS_URL", "tls://192.168.1.60:4222")
NATS_CREDS_FILE = os.getenv("NATS_CREDS_FILE", "/creds/smx-nats-monitor.creds")
NATS_CA_FILE = os.getenv("NATS_CA_FILE", "/certs/ca.crt")
DB_PATH = os.getenv("DB_PATH", "/data/monitor.db")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))
