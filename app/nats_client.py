import asyncio
import json
import ssl

import nats

from . import config, db

# SSE client queues — advisory events are broadcast here for live delivery.
sse_queues = set()

_ADVISORY_PREFIX = "$JS.EVENT.ADVISORY."


def broadcast(event):
    for q in list(sse_queues):
        try:
            q.put_nowait(event)
        except Exception:
            pass


async def connect():
    ssl_ctx = ssl.create_default_context(cafile=config.NATS_CA_FILE)
    nc = await nats.connect(
        config.NATS_URL,
        user_credentials=config.NATS_CREDS_FILE,
        tls=ssl_ctx,
        name="smx-nats-monitor",
        connect_timeout=10,
        allow_reconnect=True,
        max_reconnect_attempts=-1,
    )
    return nc


async def _request_json(nc, subject, payload=b""):
    resp = await nc.request(subject, payload, timeout=8)
    return json.loads(resp.data)


async def list_streams(nc):
    data = await _request_json(nc, "$JS.API.STREAM.LIST")
    return data.get("streams", [])


async def stream_info(nc, name):
    return await _request_json(nc, f"$JS.API.STREAM.INFO.{name}")


async def list_consumers(nc, stream):
    data = await _request_json(nc, f"$JS.API.CONSUMER.LIST.{stream}")
    return data.get("consumers", [])


async def consumer_info(nc, stream, consumer):
    return await _request_json(nc, f"$JS.API.CONSUMER.INFO.{stream}.{consumer}")


async def poll_once(nc):
    # STREAM.LIST liefert in NATS >= 2.10 volle Objekte (config + state),
    # ältere Server nur Namen — beide Formen abfangen.
    stream_names = []
    consumer_map = {}
    for entry in await list_streams(nc):
        if isinstance(entry, str):
            sname = entry
            try:
                si = await stream_info(nc, sname)
            except Exception as e:
                print(f"stream info {sname} failed: {e}")
                continue
        else:
            si = entry
            sname = (si.get("config") or {}).get("name")
        if not sname:
            continue
        stream_names.append(sname)
        consumer_names = []

        cfg = si.get("config", {})
        state = si.get("state", {})
        await db.upsert_stream(
            name=sname,
            messages=state.get("messages"),
            bytes_=state.get("bytes"),
            first_seq=state.get("first_seq"),
            last_seq=state.get("last_seq"),
            subjects=", ".join(cfg.get("subjects") or []),
            retention=str(cfg.get("retention", "")),
            created=si.get("created"),
        )
        await db.insert_snapshot(
            entity_type="stream", entity_id=sname,
            messages=state.get("messages"), bytes_=state.get("bytes"),
        )

        try:
            consumers = await list_consumers(nc, sname)
        except Exception as e:
            print(f"consumer list {sname} failed: {e}")
            consumers = []
        for centry in consumers:
            # CONSUMER.LIST: volle Objekte (mit config+num_pending) ODER nur {"name": ...}
            if isinstance(centry, str):
                cname = centry
                ci = await _safe_consumer_info(nc, sname, cname)
            elif "config" in centry and "num_pending" in centry:
                ci = centry
                cname = (centry.get("config") or {}).get("name") or centry.get("name")
            else:
                cname = centry.get("name")
                ci = await _safe_consumer_info(nc, sname, cname)
            if not cname or not ci:
                continue

            ccfg = ci.get("config", {})
            delivered = (ci.get("delivered") or {}).get("stream_seq", 0) or 0
            ack_floor = (ci.get("ack_floor") or {}).get("stream_seq", 0) or 0
            await db.upsert_consumer(
                stream=sname,
                name=cname,
                durable=ccfg.get("durable_name") or ccfg.get("name") or "",
                num_pending=ci.get("num_pending"),
                num_ack_pending=ci.get("num_ack_pending"),
                delivered=delivered,
                redelivered=ci.get("num_redelivered"),
                ack_floor=ack_floor,
            )
            await db.insert_snapshot(
                entity_type="consumer", entity_id=cname, stream=sname,
                num_pending=ci.get("num_pending"),
                num_ack_pending=ci.get("num_ack_pending"),
                delivered=delivered,
                redelivered=ci.get("num_redelivered"),
            )
            consumer_names.append(cname)
        consumer_map[sname] = consumer_names

    await db.reconcile(stream_names, consumer_map)


async def _safe_consumer_info(nc, stream, consumer):
    try:
        return await consumer_info(nc, stream, consumer)
    except Exception as e:
        print(f"consumer info {stream}/{consumer} failed: {e}")
        return None


async def poll_loop(nc):
    while True:
        try:
            await poll_once(nc)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"poll error: {e}")
        await asyncio.sleep(config.POLL_INTERVAL)


async def advisory_loop(nc):
    while True:
        try:
            sub = await nc.subscribe("$JS.EVENT.ADVISORY.>")
            async for msg in sub.messages:
                try:
                    payload = json.loads(msg.data.decode() or "{}")
                except Exception:
                    payload = {"raw": (msg.data or b"").decode(errors="replace")}
                if msg.subject == "$JS.EVENT.ADVISORY.API":
                    # API-Audit: auditiert auch unsere eigenen Polling-Reads (INFO/LIST)
                    # => Rauschen. Lebenszyklus-Events (STREAM.CREATED, CONSUMER.*, …)
                    # kommen über eigene Subjects und sind davon unabhängig.
                    continue
                event_type = msg.subject[len(_ADVISORY_PREFIX):] if msg.subject.startswith(_ADVISORY_PREFIX) else msg.subject
                event = {
                    "ts": db.now_iso(),
                    "type": event_type,
                    "stream": payload.get("stream"),
                    "consumer": payload.get("consumer"),
                    "subject": msg.subject,
                    "details": payload,
                }
                await db.insert_event(
                    type_=event_type,
                    stream=event["stream"],
                    consumer=event["consumer"],
                    subject=msg.subject,
                    details=payload,
                )
                broadcast(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"advisory error: {e}")
            await asyncio.sleep(5)
