from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a burst of messages to observe queue growth and backpressure.",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--topic", default="burst-topic")
    parser.add_argument(
        "--count",
        type=int,
        default=150,
        help="Number of publish attempts to make during the burst.",
    )
    parser.add_argument(
        "--report-every",
        type=int,
        default=10,
        help="Print queue depth and counters every N publish attempts.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Optional delay between publish attempts.",
    )
    return parser.parse_args()


def ensure_topic(client: httpx.Client, base_url: str, topic: str) -> None:
    response = client.post(f"{base_url}/topics/{topic}")
    response.raise_for_status()
    print(f"[setup] topic='{topic}' ready")


def fetch_depth(client: httpx.Client, base_url: str, topic: str) -> int:
    response = client.get(f"{base_url}/topics/{topic}/depth")
    response.raise_for_status()
    return response.json()["depth"]


def fetch_metrics(client: httpx.Client, base_url: str, topic: str) -> dict[str, object]:
    response = client.get(f"{base_url}/topics/{topic}/metrics")
    response.raise_for_status()
    return response.json()["metrics"]


def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/")

    success_count = 0
    throttled_count = 0
    error_count = 0

    try:
        with httpx.Client(timeout=5.0) as client:
            ensure_topic(client, base_url, args.topic)
            start = time.perf_counter()

            for index in range(1, args.count + 1):
                payload = {
                    "sequence": index,
                    "kind": "burst",
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                }
                response = client.post(
                    f"{base_url}/topics/{args.topic}/publish",
                    json={"payload": payload},
                )

                if response.status_code == 201:
                    success_count += 1
                elif response.status_code == 429:
                    throttled_count += 1
                else:
                    error_count += 1
                    print(
                        f"[publish-error] seq={index} status={response.status_code} "
                        f"body={response.text}"
                    )

                if index % args.report_every == 0 or response.status_code == 429:
                    depth = fetch_depth(client, base_url, args.topic)
                    metrics = fetch_metrics(client, base_url, args.topic)
                    print(
                        f"[burst-status] sent={index} accepted={success_count} "
                        f"throttled={throttled_count} depth={depth} "
                        f"published={metrics['total_published']} "
                        f"message_rate={metrics['message_rate']} "
                        f"byte_throughput={metrics['byte_throughput']}"
                    )

                if args.interval > 0:
                    time.sleep(args.interval)

            elapsed = time.perf_counter() - start
            final_depth = fetch_depth(client, base_url, args.topic)
            final_metrics = fetch_metrics(client, base_url, args.topic)
            print(
                "[burst-summary] "
                f"attempted={args.count} accepted={success_count} "
                f"throttled={throttled_count} errors={error_count} "
                f"final_depth={final_depth} elapsed_seconds={elapsed:.3f}"
            )
            print(f"[burst-metrics] {final_metrics}")

    except httpx.RequestError as exc:
        print(f"[connection-error] could not reach broker: {exc}")
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        print("[stopped] burst test interrupted by user")


if __name__ == "__main__":
    main()
