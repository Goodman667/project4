from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from itertools import count

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously publish demo messages to one topic.",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--topic", default="demo-topic")
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of messages to send. Use 0 for infinite publishing.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Seconds to wait between publish calls.",
    )
    parser.add_argument(
        "--prefix",
        default="demo-message",
        help="Text prefix for each message payload.",
    )
    return parser.parse_args()


def ensure_topic(client: httpx.Client, base_url: str, topic: str) -> None:
    response = client.post(f"{base_url}/topics/{topic}")
    response.raise_for_status()
    print(f"[setup] topic='{topic}' ready")


def build_payload(prefix: str, index: int) -> dict[str, object]:
    return {
        "sequence": index,
        "text": f"{prefix}-{index}",
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/")

    try:
        with httpx.Client(timeout=5.0) as client:
            ensure_topic(client, base_url, args.topic)

            sent_count = 0
            iterator = count(1) if args.count == 0 else range(1, args.count + 1)

            for index in iterator:
                payload = build_payload(args.prefix, index)
                response = client.post(
                    f"{base_url}/topics/{args.topic}/publish",
                    json={"payload": payload},
                )

                if response.status_code == 201:
                    sent_count += 1
                    body = response.json()
                    print(
                        f"[publish-ok] topic='{args.topic}' "
                        f"seq={index} message_id={body['message']['id']}"
                    )
                elif response.status_code == 429:
                    print(
                        f"[publish-throttled] topic='{args.topic}' seq={index} "
                        "queue is full enough to reject new messages"
                    )
                else:
                    print(
                        f"[publish-error] status={response.status_code} "
                        f"body={response.text}"
                    )

                if args.interval > 0:
                    time.sleep(args.interval)

    except httpx.RequestError as exc:
        print(f"[connection-error] could not reach broker: {exc}")
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        print("[stopped] producer demo interrupted by user")
    else:
        print(f"[done] producer demo finished, messages_sent={sent_count}")


if __name__ == "__main__":
    main()
