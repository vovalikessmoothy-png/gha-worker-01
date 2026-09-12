// Cloudflare Worker — API-шлюз кластера + двунаправленный жизненный цикл сессий.
//
// Два контура:
//
// 1) Очередь батч-задач (warm-pool), как было — односторонний шлюз→воркер:
//    POST /submit    { profile, task }  -> { id }          клиент кладёт задачу
//    GET  /pull?profile=embed           -> задача | 204     warm-воркер тянет одну
//    GET  /pull?profile=llm&max=5       -> {jobs:[…]} | 204  батч: до max задач в одну VM
//    POST /result    { id, result }                         воркер отдаёт результат
//    POST /results   { results:[{id,result|error}] }        батч-результат разом
//    GET  /result/{id}                  -> { status,result} клиент забирает
//
// 2) Жизненный цикл агент-сессии (двунаправленный, на Durable Object).
//    Мутации клиент→шлюз (идемпотентны по Idempotency-Key):
//    POST /session/{id}/run    { startIntent, doneCriterion }  старт/догон прогона
//    POST /session/{id}/reply  { message }                     инъекция сообщения
//    POST /session/{id}/nudge  { reason? }                     пинок follower-кроном
//    Колбэки агент→DO (закрывают контракт, без них нет коалесинга и стопа крона):
//    POST /session/{id}/started { runId }
//    POST /session/{id}/idle    { runId }
//    POST /session/{id}/done    { runId, report }
//    Якорь состояния:
//    GET  /session/{id}/state  -> { status, startIntent, doneCriterion,
//                                   last3reports, followerActive, runId }
//
// 3) Durable-кампания (мега-батч 1000+ задач с чекпоинтом и авто-резюмом).
//    POST /campaign  { profile, inputs:[…], batch? }  -> { id, total }
//    POST /campaign/{id}/tick   -> { done, cursor, inflight, remaining, finished }
//    GET  /campaign/{id}/state  -> прогресс без побочек
//    GET  /campaign/{id}/results-> { results:[…] } по порядку входов
//    Кампания хранит массив входов + курсор в Durable Object (переживает
//    рестарты). Cron-триггер воркера раз в минуту тикает все активные кампании:
//    дозаполняет очередь до окна CAMPAIGN_WINDOW, собирает готовые результаты,
//    двигает курсор — и так до конца массива. Warm-пул GHA тянет задачи как
//    обычные (/pull → /result), кампания их клеймит и агрегирует.
//
// Авторизация: Authorization: Bearer <QUEUE_KEY> на всех маршрутах.
// Идемпотентность мутаций: заголовок Idempotency-Key (или body.idempotencyKey).

const enc = (o, status = 200) =>
  new Response(JSON.stringify(o), {
    status,
    headers: { "content-type": "application/json" },
  });

function auth(req, env) {
  const h = req.headers.get("authorization") || "";
  return env.QUEUE_KEY && h === `Bearer ${env.QUEUE_KEY}`;
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const p = url.pathname;

    if (!auth(req, env)) return new Response("unauthorized", { status: 401 });

    // ---------- контур 2: сессии -> Durable Object ----------
    // /session/{id}/... маршрутизируем в DO, инстанс на session id.
    const sm = p.match(/^\/session\/([^/]+)(\/.*)?$/);
    if (sm) {
      const sessionId = decodeURIComponent(sm[1]);
      const doId = env.SESSION.idFromName(sessionId);
      const stub = env.SESSION.get(doId);
      // прокидываем «хвост» пути в DO как его внутренний маршрут
      const inner = new URL(req.url);
      inner.pathname = sm[2] || "/state";
      return stub.fetch(new Request(inner.toString(), req));
    }

    // ---------- контур 3: durable-кампании -> CampaignDO ----------
    // Создание: генерим id, регистрируем в наборе активных (его тикает cron),
    // проксируем тело в CampaignDO /create.
    if (req.method === "POST" && p === "/campaign") {
      const id = crypto.randomUUID();
      const stub = env.CAMPAIGN.get(env.CAMPAIGN.idFromName(id));
      const inner = new URL(req.url);
      inner.pathname = "/create";
      const res = await stub.fetch(new Request(inner.toString(), req));
      const data = await res.json().catch(() => ({}));
      if (res.status < 300) await env.RESULTS.put(`camp:${id}`, "1"); // активна для cron
      return enc({ id, ...data }, res.status);
    }
    const cm = p.match(/^\/campaign\/([^/]+)(\/.*)?$/);
    if (cm) {
      const cid = decodeURIComponent(cm[1]);
      const stub = env.CAMPAIGN.get(env.CAMPAIGN.idFromName(cid));
      const inner = new URL(req.url);
      inner.pathname = cm[2] || "/state";
      return stub.fetch(new Request(inner.toString(), req));
    }

    // ---------- контур 1: очередь батч-задач ----------
    if (req.method === "POST" && p === "/submit") {
      const { profile, task } = await req.json();
      const id = crypto.randomUUID();
      await env.QUEUE.put(`task:${profile}:${id}`, JSON.stringify({ id, task }), {
        expirationTtl: 3600,
      });
      await env.RESULTS.put(id, JSON.stringify({ status: "queued" }), {
        expirationTtl: 3600,
      });
      return enc({ id });
    }

    if (req.method === "GET" && p === "/pull") {
      const profile = url.searchParams.get("profile") || "embed";
      // max — сколько задач забрать в одну VM за раз (батч). Клампим 1..25.
      const max = Math.min(Math.max(parseInt(url.searchParams.get("max") || "1", 10) || 1, 1), 25);
      const list = await env.QUEUE.list({ prefix: `task:${profile}:`, limit: max });
      if (!list.keys.length) return new Response(null, { status: 204 });

      // Клеймим задачи атомарно-по-ключу: delete-after-read = задача уходит одному
      // воркеру. Параллельный /pull другого слота увидит уже удалённый ключ (val=null).
      const jobs = [];
      for (const k of list.keys) {
        const val = await env.QUEUE.get(k.name);
        if (!val) continue; // забрал другой слот между list и get — пропускаем
        await env.QUEUE.delete(k.name);
        const { id, task } = JSON.parse(val);
        await env.RESULTS.put(id, JSON.stringify({ status: "running" }), {
          expirationTtl: 3600,
        });
        jobs.push({ id, task });
      }
      if (!jobs.length) return new Response(null, { status: 204 });

      // Обратная совместимость: max=1 → плоский {id,task}; max>1 → {jobs:[…]}.
      return url.searchParams.has("max") ? enc({ jobs }) : enc(jobs[0]);
    }

    if (req.method === "POST" && p === "/result") {
      const { id, result, error } = await req.json();
      await env.RESULTS.put(
        id,
        JSON.stringify({ status: error ? "error" : "done", result, error }),
        { expirationTtl: 3600 }
      );
      return enc({ ok: true });
    }

    if (req.method === "POST" && p === "/results") {
      // батч-возврат: массив результатов одним запросом (меньше round-trip'ов).
      const { results } = await req.json();
      if (!Array.isArray(results)) return enc({ ok: false, error: "results[] required" }, 400);
      await Promise.all(
        results.map(({ id, result, error }) =>
          env.RESULTS.put(
            id,
            JSON.stringify({ status: error ? "error" : "done", result, error }),
            { expirationTtl: 3600 }
          )
        )
      );
      return enc({ ok: true, count: results.length });
    }

    if (req.method === "GET" && p.startsWith("/result/")) {
      const id = p.slice("/result/".length);
      const val = await env.RESULTS.get(id);
      if (!val) return new Response("not found", { status: 404 });
      return new Response(val, { headers: { "content-type": "application/json" } });
    }

    return new Response("ok"); // healthcheck
  },

  // Cron-триггер: тикаем все активные кампании. Durable-резюм — состояние живёт
  // в CampaignDO, поэтому рестарт воркера/аккаунта ничего не теряет: следующий
  // тик просто продолжит с курсора. Завершённые кампании снимаем с учёта.
  async scheduled(event, env, ctx) {
    const active = await env.RESULTS.list({ prefix: "camp:" });
    for (const k of active.keys) {
      const cid = k.name.slice("camp:".length);
      const stub = env.CAMPAIGN.get(env.CAMPAIGN.idFromName(cid));
      const res = await stub.fetch(
        new Request("https://do/tick", { method: "POST", body: "{}" })
      );
      const data = await res.json().catch(() => ({}));
      if (data.finished) await env.RESULTS.delete(k.name);
    }
  },
};

// ============================================================================
// Durable Object: координация одной агент-сессии.
// Гарантии: сериализация запросов на один инстанс (DO однопоточен на id),
// поэтому коалесинг и дедуп идемпотентности не требуют внешних локов.
// ============================================================================

const IDEMP_TTL_MS = 24 * 3600 * 1000; // помним ключи идемпотентности сутки
const MAX_REPORTS = 3; // якорь: последние 3 отчёта

export class SessionDO {
  constructor(state) {
    this.state = state;
    this.store = state.storage;
  }

  async _load() {
    const s = (await this.store.get("session")) || {
      status: "idle", // idle | queued | running | done
      startIntent: null,
      doneCriterion: null,
      runId: null,
      last3reports: [],
      pendingIntent: null, // накопленный коалесингом догон
      followerActive: false, // нужен ли follower-крон пинать сессию
      updatedAt: 0,
    };
    return s;
  }

  async _save(s, now) {
    s.updatedAt = now;
    await this.store.put("session", s);
  }

  // --- идемпотентность: вернуть кэш или null ---
  async _idemLookup(key) {
    if (!key) return null;
    const rec = await this.store.get(`idem:${key}`);
    return rec || null; // { status, body }
  }
  async _idemSave(key, status, body, now) {
    if (!key) return;
    await this.store.put(`idem:${key}`, { status, body, ts: now });
    // один отложенный будильник на уборку; если уже стоит — не двигаем
    if ((await this.store.getAlarm()) == null) {
      await this.store.setAlarm(now + IDEMP_TTL_MS);
    }
  }

  async alarm() {
    // уборка протухших ключей идемпотентности по возрасту.
    const now = Date.now();
    const all = await this.store.list({ prefix: "idem:" });
    let alive = 0;
    for (const [k, v] of all) {
      if (!v || now - (v.ts || 0) >= IDEMP_TTL_MS) await this.store.delete(k);
      else alive++;
    }
    // если ещё остались ключи — переставляем будильник на следующий цикл
    if (alive > 0) await this.store.setAlarm(now + IDEMP_TTL_MS);
  }

  async fetch(req) {
    const url = new URL(req.url);
    const route = url.pathname; // /run /reply /nudge /started /idle /done /state
    const now = Date.now();
    const idemKey = req.headers.get("idempotency-key") || null;

    // ---- чтение состояния ----
    if (req.method === "GET" && route === "/state") {
      const s = await this._load();
      return enc({
        status: s.status,
        startIntent: s.startIntent,
        doneCriterion: s.doneCriterion,
        last3reports: s.last3reports,
        followerActive: s.followerActive,
        runId: s.runId,
        pendingIntent: s.pendingIntent,
      });
    }

    if (req.method !== "POST") {
      return new Response("method not allowed", { status: 405 });
    }

    const body = await req.json().catch(() => ({}));
    const key = idemKey || body.idempotencyKey || null;

    // дедуп мутаций: тот же ключ -> тот же ответ, без повторного эффекта
    const isMutation = ["/run", "/reply", "/nudge"].includes(route);
    if (isMutation) {
      const cached = await this._idemLookup(key);
      if (cached) return enc(cached.body, cached.status);
    }

    const s = await this._load();
    let out, status = 200;

    switch (route) {
      // ---------- мутации клиент→шлюз ----------
      case "/run": {
        // Коалесинг 1.3: если прогон уже идёт — не плодим дубль-сессию,
        // складываем намерение в pending; агент догонит на idle/done.
        const intent = body.startIntent ?? null;
        if (body.doneCriterion != null) s.doneCriterion = body.doneCriterion;
        if (s.status === "running" || s.status === "queued") {
          s.pendingIntent = intent;
          out = { ok: true, coalesced: true, runId: s.runId, status: s.status };
        } else {
          s.status = "queued";
          s.startIntent = intent;
          s.runId = crypto.randomUUID();
          s.followerActive = true; // до первого idle/done крон сторожит прогон
          out = { ok: true, coalesced: false, runId: s.runId, status: s.status };
        }
        break;
      }
      case "/reply": {
        // инъекция сообщения в живую сессию; если спала — будит как догон
        if (s.status === "idle" || s.status === "done") {
          s.status = "queued";
          s.runId = crypto.randomUUID();
          s.followerActive = true;
        }
        s.pendingIntent = body.message ?? s.pendingIntent;
        out = { ok: true, runId: s.runId, status: s.status };
        break;
      }
      case "/nudge": {
        // пинок follower-кроном: повторно будим, только если реально подвисли
        if (s.status === "queued" && s.followerActive) {
          out = { ok: true, requeued: true, runId: s.runId };
        } else {
          out = { ok: true, requeued: false, status: s.status };
        }
        break;
      }

      // ---------- колбэки агент→DO ----------
      case "/started": {
        if (body.runId && body.runId !== s.runId) {
          out = { ok: false, reason: "stale-runId", current: s.runId };
          status = 409;
          break;
        }
        s.status = "running";
        out = { ok: true };
        break;
      }
      case "/idle": {
        if (body.runId && body.runId !== s.runId) {
          out = { ok: false, reason: "stale-runId", current: s.runId };
          status = 409;
          break;
        }
        // агент встал в ожидание: стоп follower-крону + разворачиваем коалесинг
        if (s.pendingIntent != null) {
          s.status = "queued";
          s.startIntent = s.pendingIntent;
          s.pendingIntent = null;
          s.runId = crypto.randomUUID();
          s.followerActive = true;
          out = { ok: true, pickup: true, runId: s.runId };
        } else {
          s.status = "idle";
          s.followerActive = false; // нечего сторожить
          out = { ok: true, pickup: false };
        }
        break;
      }
      case "/done": {
        if (body.runId && body.runId !== s.runId) {
          out = { ok: false, reason: "stale-runId", current: s.runId };
          status = 409;
          break;
        }
        if (body.report != null) {
          s.last3reports = [body.report, ...s.last3reports].slice(0, MAX_REPORTS);
        }
        if (s.pendingIntent != null) {
          // за время прогона накопился догон — сразу в новый прогон
          s.status = "queued";
          s.startIntent = s.pendingIntent;
          s.pendingIntent = null;
          s.runId = crypto.randomUUID();
          s.followerActive = true;
          out = { ok: true, pickup: true, runId: s.runId };
        } else {
          s.status = "done";
          s.followerActive = false; // прогон закрыт — крон-фолловер останавливается
          out = { ok: true, pickup: false };
        }
        break;
      }

      default:
        return new Response("not found", { status: 404 });
    }

    await this._save(s, now);
    if (isMutation) await this._idemSave(key, status, out, now);
    return enc(out, status);
  }
}

// ============================================================================
// Durable Object: одна долгая кампания (мега-батч на 1000+ задач).
// Массив входов хранится страницами (лимит значения DO ~128 КБ), прогресс —
// курсор + карта in-flight задач. Тик: (1) собрать готовые результаты из RESULTS
// обратно в кампанию и освободить слоты; (2) дозаполнить очередь до окна;
// (3) двигать курсор. Идемпотентно к рестартам: всё состояние в storage.
// ============================================================================

const CAMPAIGN_WINDOW = 200; // сколько задач кампании держим в очереди одновременно
const CAMPAIGN_PAGE = 100; // входов на одну страницу storage

export class CampaignDO {
  constructor(state, env) {
    this.state = state;
    this.store = state.storage;
    this.env = env;
  }

  async _meta() {
    return (
      (await this.store.get("meta")) || {
        profile: "embed",
        total: 0,
        cursor: 0, // сколько входов уже поставлено в очередь
        done: 0, // сколько результатов собрано
        batch: 1,
        createdAt: 0,
        finishedAt: null,
        inflight: {}, // taskId -> индекс входа
      }
    );
  }

  async _input(idx) {
    const page = await this.store.get(`in:${Math.floor(idx / CAMPAIGN_PAGE)}`);
    return page ? page[idx % CAMPAIGN_PAGE] : null;
  }

  // Поставить вход idx в общую очередь как обычную задачу (warm-пул её вытянет
  // через /pull, вернёт через /result). Помечаем как in-flight.
  async _enqueueOne(meta, idx) {
    const input = await this._input(idx);
    const tid = crypto.randomUUID();
    await this.env.QUEUE.put(
      `task:${meta.profile}:${tid}`,
      JSON.stringify({ id: tid, task: input }),
      { expirationTtl: 3600 }
    );
    await this.env.RESULTS.put(tid, JSON.stringify({ status: "queued" }), {
      expirationTtl: 3600,
    });
    meta.inflight[tid] = idx;
  }

  async fetch(req) {
    const url = new URL(req.url);
    const route = url.pathname; // /create /tick /state /results
    const now = Date.now();

    if (req.method === "POST" && route === "/create") {
      const { profile, inputs, batch } = await req.json().catch(() => ({}));
      if (!Array.isArray(inputs) || !inputs.length) {
        return enc({ ok: false, error: "inputs[] required" }, 400);
      }
      for (let i = 0; i < inputs.length; i += CAMPAIGN_PAGE) {
        await this.store.put(`in:${i / CAMPAIGN_PAGE}`, inputs.slice(i, i + CAMPAIGN_PAGE));
      }
      const meta = {
        profile: profile || "embed",
        total: inputs.length,
        cursor: 0,
        done: 0,
        batch: Math.max(1, batch || 1),
        createdAt: now,
        finishedAt: null,
        inflight: {},
      };
      await this.store.put("meta", meta);
      return enc({ ok: true, total: meta.total });
    }

    const meta = await this._meta();

    if (req.method === "POST" && route === "/tick") {
      // 1) собрать готовое из RESULTS, освободить слоты; потерянные — переклеймить
      for (const tid of Object.keys(meta.inflight)) {
        const v = await this.env.RESULTS.get(tid);
        if (!v) {
          // TTL истёк / результат пропал — вернём этот вход в очередь заново
          const idx = meta.inflight[tid];
          delete meta.inflight[tid];
          await this._enqueueOne(meta, idx);
          continue;
        }
        const r = JSON.parse(v);
        if (r.status === "done" || r.status === "error") {
          const idx = meta.inflight[tid];
          await this.store.put(
            `out:${idx}`,
            r.status === "error" ? { error: r.error } : r.result
          );
          delete meta.inflight[tid];
          meta.done += 1;
          await this.env.RESULTS.delete(tid); // прибираем KV
        }
      }
      // 2) дозаполнить окно новыми задачами от курсора до конца массива
      while (Object.keys(meta.inflight).length < CAMPAIGN_WINDOW && meta.cursor < meta.total) {
        await this._enqueueOne(meta, meta.cursor);
        meta.cursor += 1;
      }
      const finished = meta.done >= meta.total;
      if (finished && !meta.finishedAt) meta.finishedAt = now;
      await this.store.put("meta", meta);
      return enc({
        ok: true,
        total: meta.total,
        done: meta.done,
        cursor: meta.cursor,
        inflight: Object.keys(meta.inflight).length,
        remaining: meta.total - meta.done,
        finished,
      });
    }

    if (req.method === "GET" && route === "/state") {
      return enc({
        profile: meta.profile,
        total: meta.total,
        done: meta.done,
        cursor: meta.cursor,
        inflight: Object.keys(meta.inflight).length,
        finished: meta.done >= meta.total,
        finishedAt: meta.finishedAt,
      });
    }

    if (req.method === "GET" && route === "/results") {
      const results = [];
      for (let i = 0; i < meta.total; i++) {
        results.push((await this.store.get(`out:${i}`)) ?? null);
      }
      return enc({ total: meta.total, done: meta.done, results });
    }

    return new Response("not found", { status: 404 });
  }
}
