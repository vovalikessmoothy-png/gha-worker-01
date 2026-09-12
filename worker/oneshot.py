"""One-shot: посчитать одну задачу из client_payload и вернуть по callback_url."""
import os, json, urllib.request
from loop import load_model, infer  # переиспользуем рантайм

PROFILE      = os.environ.get("PROFILE", "embed")
TASK_JSON    = os.environ.get("TASK_JSON", "{}")
CALLBACK_URL = os.environ.get("CALLBACK_URL")
QUEUE_KEY    = os.environ.get("QUEUE_KEY", "")


def main():
    task = json.loads(TASK_JSON)
    model = load_model(PROFILE)
    try:
        result = infer(model, task)
        payload = {"result": result}
    except Exception as e:
        payload = {"error": str(e)}
    if CALLBACK_URL:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            CALLBACK_URL, data=data, method="POST",
            headers={"Authorization": f"Bearer {QUEUE_KEY}", "Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=30).read()
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
