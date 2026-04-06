from __future__ import annotations

import argparse
import time

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously consume messages from one topic and ack them.",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--topic", default="demo-topic")
    parser.add_argument("--consumer-id", default="consumer-1")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds to wait before polling again when the queue is empty.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Maximum number of messages to consume. Use 0 for infinite polling.",
    )
    return parser.parse_args()


def ensure_topic(client: httpx.Client, base_url: str, topic: str) -> None:
    response = client.post(f"{base_url}/topics/{topic}")
    response.raise_for_status()
    print(f"[setup] topic='{topic}' ready")


def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    acked_count = 0

    try:
        with httpx.Client(timeout=5.0) as client:
            ensure_topic(client, base_url, args.topic)

            while True:
                if args.max_messages and acked_count >= args.max_messages:
                    break

                response = client.get(
                    f"{base_url}/topics/{args.topic}/consume",
                    params={"consumer_id": args.consumer_id},
                )
                response.raise_for_status()
                body = response.json()

                if body.get("message") is None:
                    print(
                        f"[idle] topic='{args.topic}' no messages available, "
                        f"sleeping {args.poll_interval}s"
                    )
                    time.sleep(args.poll_interval)
                    continue

                message = body["message"]
                message_id = message["id"]
                print(
                    f"[consume-ok] consumer='{args.consumer_id}' "
                    f"message_id={message_id} retry_count={message['retry_count']} "
                    f"payload={message['payload']}"
                )

                ack_response = client.post(f"{base_url}/messages/{message_id}/ack")
                ack_response.raise_for_status()
                acked_count += 1
                print(f"[ack-ok] message_id={message_id} total_acked={acked_count}")

    except httpx.RequestError as exc:
        print(f"[connection-error] could not reach broker: {exc}")
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        print("[stopped] consumer demo interrupted by user")
    else:
        print(f"[done] consumer demo finished, total_acked={acked_count}")


if __name__ == "__main__":
    main()
