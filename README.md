# gha-compute-cluster

Почти-бесплатный вычислительный кластер на бесплатных раннерах GitHub Actions
для публичных репозиториев. Своё API поверх — либо через Cloudflare Worker,
либо напрямую триггером GitHub Actions.

> Только легальные лимиты бесплатных тарифов. Публичные репо → Actions без лимита
> минут. Никакого обхода биллинга/ToS.

## Что даёт бесплатный тариф (реальность)
- Публичный репо → Actions-минуты **бесплатны и без лимита**.
- **20 одновременных jobs** на аккаунт (standard Linux hosted runners).
- Раннер: **2 vCPU, ~7 ГБ RAM, ~14 ГБ SSD**, только **CPU** (GPU нет).
- Один job живёт **до 6 часов**, весь workflow — до 35 дней (с ожиданием).
- 20 репо × 20 jobs = **~400 параллельных воркеров** на аккаунт.

## Важно про задержку (правка к ТЗ ~5 сек)
Холодный старт job'а = очередь + провижн раннера = **обычно 15–45 сек**, не 5.
Уложиться в ~5 сек на запрос можно только **тёплым пулом**:

- **Warm-pool** (рекомендую): держим N долгоживущих jobs (до 6 ч каждый),
  каждый в цикле опрашивает очередь задач и обрабатывает уже прогретой моделью.
  Задержка на запрос ≈ poll-интервал (1–3 сек) + инференс. Перезапуск воркера
  до истечения 6 ч.
- **One-shot** (`repository_dispatch`/`workflow_dispatch`): job на задачу. Просто,
  но холодный старт 15–45 сек — годится для батчей, не для realtime.

### Батч-запуск (несколько задач в одну VM)
Когда инференс долгий (LLM), тянуть задачи по одной расточительно: прогрев весов
и накладные round-trip'ы не амортизируются. Warm-воркер умеет забирать **пачку**
задач за один pull и считать их батч-форвардом (5 промптов в один проход вместо
пяти отдельных):

- `GET /pull?profile=llm-small&max=5` → `{jobs:[…]}` (до 5 задач, кламп 1..25).
  Клейм атомарный по ключу: задача уходит ровно одному слоту.
- `POST /results { results:[{id,result|error}] }` — вернуть весь батч одним запросом.
- Воркер: `BATCH=5` (env / input `batch` в workflow). `BATCH=1` — старое поведение.
- Реальный батч-форвард включается в `worker/loop.py::infer_batch` (embed —
  `model.embed([...])`; llm — `llama_cpp` c `n_parallel` / continuous batching).

Компромисс: больший `max` = выше throughput, но и выше latency первой задачи
(ждём набора пачки) и риск потерять весь батч при падении слота. Для realtime — `max=1`.

## Архитектура
```
клиент ──HTTP──▶ Cloudflare Worker (API + очередь + хранилище результатов)
                       │  задачи в очередь (KV / D1 / Queue)
                       ▼
        ┌──────────── warm-pool воркеры ────────────┐
        │  20 репо × до 20 jobs = до 400 воркеров    │
        │  каждый: loop { pull task → infer → push } │
        └────────────────────────────────────────────┘
                       ▲  результат обратно в Worker
клиент ◀──HTTP── poll /result/{id}  (или SSE/webhook)
```

Две точки входа в кластер:
1. **Cloudflare Worker** — единый REST API, очередь, дедлайны, авторизация.
   Идеально для warm-pool: воркеры сами тянут задачи, Worker не зависит от
   GitHub rate-limit на dispatch.
2. **Прямой GitHub dispatch** — `POST /repos/{o}/{r}/dispatches`
   (`repository_dispatch`) или `workflow_dispatch`. Хуки без Cloudflare, но
   лимит ~1000 dispatch/час и холодный старт.

## Профиль пригодных задач
Дёшево, массово-параллельно, не realtime-критично, терпит ~5 с старт + ~5 с вывод:
батч-инференс лёгких моделей (эмбеддинги, классификация, реранк, ASR, суммаризация
короткими LLM), рендер, кроулинг, перекодирование, массовые расчёты.

## API-шпаргалка
```
# контур 1: очередь батч-задач
POST /submit   {profile,task}                 -> {id}
GET  /pull?profile=X                          -> {id,task} | 204        (одна)
GET  /pull?profile=X&max=5                     -> {jobs:[{id,task}…]} | 204 (батч)
POST /result   {id,result|error}                                        (одна)
POST /results  {results:[{id,result|error}…]}                           (батч)
GET  /result/{id}                             -> {status,result|error}

# контур 2: жизненный цикл агент-сессии (Durable Object), идемпотентно по Idempotency-Key
POST /session/{id}/run    {startIntent,doneCriterion}
POST /session/{id}/reply  {message}
POST /session/{id}/nudge  {reason?}
POST /session/{id}/{started|idle|done}  {runId,report?}
GET  /session/{id}/state

# контур 3: durable-кампания — мега-батч 1000+ задач с чекпоинтом и авто-резюмом
POST /campaign            {profile,inputs:[…],batch?}  -> {id,total}
GET  /campaign/{id}/state                              -> {done,total,cursor,inflight,finished}
GET  /campaign/{id}/results                            -> {results:[…]}  (по порядку входов)
POST /campaign/{id}/tick                               -> (дёргает cron сам, вручную не нужно)
```

### Durable-кампания (мега-батч)
Для «прогнать 1000+ расчётов до конца, что бы ни случилось» есть отдельный контур.
Кладёшь весь массив входов одним `POST /campaign` — он оседает в Durable Object
(массив страницами + курсор + карта in-flight). Cron-триггер воркера **раз в
минуту** тикает все активные кампании: дозаполняет очередь до окна (`CAMPAIGN_WINDOW`,
200 задач в полёте), собирает готовые результаты обратно, двигает курсор — и так до
конца массива. Warm-пул GHA тянет эти задачи как обычные (`/pull`→`/result`).
Переживает рестарты (состояние в DO), потерянные по TTL задачи переклеймиваются.
Забираешь всё разом через `/campaign/{id}/results` когда `finished:true`.
Авторизация: `Authorization: Bearer <QUEUE_KEY>` на всех маршрутах.

## Раскладка репозиториев
- **5 воркер-репо** (`gha-worker-01…05`, публичные) — каждый по **9 одновременных
  jobs** (мягкий лимит, чтобы не упираться в потолок аккаунта): 5 × 9 = **45 тёплых
  воркеров**. В каждом секреты `QUEUE_URL` + `QUEUE_KEY`, содержимое = этот скелет.
- **6-й репо — оркестратор** (`gha-cluster-orchestrator`): держит код самого API
  (`cloudflare/worker.js` — очередь + сессии + кампании), деплоится в Cloudflare.

## Каталог
- `.github/workflows/worker.yml` — тёплый воркер-loop (warm-pool, вход `batch`, 9 слотов).
- `.github/workflows/oneshot.yml` — one-shot job по dispatch.
- `cloudflare/worker.js` — API-шлюз + очередь на KV.
- `MODELS.md` — матрица моделей под 7 ГБ RAM / CPU и их назначение.
- `SETUP.md` — как поднять с нуля.
