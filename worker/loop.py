"""Warm-pool воркер. Грузит модель один раз, потом в цикле тянет задачи из
очереди Cloudflare Worker, считает, возвращает результат. Живёт LIFETIME минут,
затем корректно выходит (schedule/dispatch поднимет новый пул)."""
import os, time, json, urllib.request

QUEUE_URL = os.environ["QUEUE_URL"].rstrip("/")
QUEUE_KEY = os.environ["QUEUE_KEY"]
PROFILE   = os.environ.get("PROFILE", "embed")
LIFETIME  = int(os.environ.get("LIFETIME", "340")) * 60
# BATCH — сколько задач тянуть в одну VM за раз. Для долгого LLM-инференса
# батч амортизирует прогрев/загрузку весов и даёт батч-форвард (5 задач в один
# проход) вместо пяти round-trip'ов. 1 = старое поведение (по одной).
BATCH     = int(os.environ.get("BATCH", "1"))
POLL_SEC  = 1.5

HDR = {"Authorization": f"Bearer {QUEUE_KEY}", "Content-Type": "application/json"}


def _req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(QUEUE_URL + path, data=data, headers=HDR, method=method)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            if resp.status == 204:
                return None
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        if e.code == 204:
            return None
        raise


def load_model(profile):
    """Заглушка загрузки. Заменить на реальный рантайм под профиль:
    embed -> onnxruntime + tokenizer; llm-* -> llama_cpp.Llama(...); asr -> whisper.cpp."""
    print(f"[slot {os.environ.get('SLOT')}] loading profile={profile} ...", flush=True)
    # пример для embed:
    #   from fastembed import TextEmbedding
    #   return TextEmbedding("BAAI/bge-small-en-v1.5")
    return {"profile": profile}


def infer(model, task):
    """Заменить на реальный инференс по профилю."""
    if model["profile"] == "embed":
        # emb = list(model.embed(task["inputs"]))
        return {"echo": task, "note": "wire real embed here"}
    return {"echo": task}


def infer_batch(model, tasks):
    """Батч-инференс: список задач -> список результатов (порядок сохраняется).
    Дефолт — по одной через infer(). Заменить на реальный батч-форвард:
      embed -> model.embed([t["inputs"] for t in tasks]) одним проходом;
      llm-* -> llama_cpp с n_parallel / continuous batching, либо vLLM-стиль.
    Ошибка одной задачи не роняет батч: возвращаем {"error": …} на её месте."""
    out = []
    for t in tasks:
        try:
            out.append({"result": infer(model, t)})
        except Exception as e:
            out.append({"error": str(e)})
    return out


def main():
    model = load_model(PROFILE)
    deadline = time.time() + LIFETIME
    print(f"warm worker up, profile={PROFILE}, lifetime={LIFETIME}s, batch={BATCH}", flush=True)
    while time.time() < deadline:
        # BATCH=1 → плоский {id,task}; BATCH>1 → {jobs:[…]}. Нормализуем в список.
        if BATCH > 1:
            resp = _req("GET", f"/pull?profile={PROFILE}&max={BATCH}")
            jobs = (resp or {}).get("jobs", [])
        else:
            job = _req("GET", f"/pull?profile={PROFILE}")
            jobs = [job] if job else []

        if not jobs:
            time.sleep(POLL_SEC)
            continue

        outs = infer_batch(model, [j["task"] for j in jobs])
        results = [{"id": j["id"], **o} for j, o in zip(jobs, outs)]
        if len(results) == 1:
            _req("POST", "/result", results[0])
        else:
            _req("POST", "/results", {"results": results})  # один round-trip на батч
    print("lifetime reached, exiting for restart", flush=True)


if __name__ == "__main__":
    main()
