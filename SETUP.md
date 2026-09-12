# Как поднять кластер с нуля

## 0. Починить GitHub-токен (сейчас 401)
Текущий токен невалиден — API создание репо не проходит. Обнови classic-токен
(scopes: `repo`, `read:org`, для dispatch ещё `workflow`) через форму бота, затем
я создам репозитории и запушу этот скелет автоматически.

## 1. Cloudflare Worker (API + очередь)
```
cd cloudflare
npx wrangler kv namespace create QUEUE
npx wrangler kv namespace create RESULTS
# подставь id в wrangler.toml
npx wrangler secret put QUEUE_KEY      # общий секрет
npx wrangler deploy
```
Получишь URL вида `https://gha-cluster-api.<acc>.workers.dev`.

## 2. Репозитории-воркеры (публичные!)
- Создать 1..20 публичных репо (`gha-worker-01 ... -20`) — по 20 jobs каждый.
- В каждом repo → Settings → Secrets:
  - `QUEUE_URL` = URL Worker'а из шага 1
  - `QUEUE_KEY` = тот же секрет
- Запушить в каждый содержимое этого скелета.

## 3. Запуск warm-пула
Для каждого репо и профиля:
```
gh workflow run worker.yml -f profile=embed -f lifetime_min=340
```
Cron в worker.yml сам перезапускает пул каждые 5 часов.

## 4. Использование API
```
# положить задачу
curl -H "Authorization: Bearer $KEY" -X POST $URL/submit \
  -d '{"profile":"embed","task":{"inputs":["hello","world"]}}'
# -> {"id":"..."}

# забрать результат
curl -H "Authorization: Bearer $KEY" $URL/result/<id>
```

## 5. Жизненный цикл агент-сессии (Durable Object)
Второй контур API — двунаправленный, на Durable Object (инстанс на session id).

Деплой (нужен CF-аккаунт):
```
wrangler kv namespace create QUEUE     # подставь id в wrangler.toml
wrangler kv namespace create RESULTS
wrangler secret put QUEUE_KEY          # bearer для очереди и сессий
wrangler deploy                        # применит migration v1 (SessionDO)
```

Контракт (все мутации идемпотентны по заголовку `Idempotency-Key`):
```
# старт прогона
curl -H "Authorization: Bearer $KEY" -H "Idempotency-Key: run-1" \
  -X POST $URL/session/s1/run \
  -d '{"startIntent":"собери отчёт","doneCriterion":"есть ссылка"}'
# -> {"ok":true,"coalesced":false,"runId":"...","status":"queued"}

# колбэки агента
curl -H "Authorization: Bearer $KEY" -X POST $URL/session/s1/started -d '{"runId":"..."}'
curl -H "Authorization: Bearer $KEY" -X POST $URL/session/s1/done \
  -d '{"runId":"...","report":"готово: https://..."}'

# якорь состояния
curl -H "Authorization: Bearer $KEY" $URL/session/s1/state
# -> {status, startIntent, doneCriterion, last3reports, followerActive, runId}
```

Гарантии:
- **Коалесинг 1.3** — `run`/`reply` во время активного прогона не плодят дубль-сессию,
  а складываются в `pendingIntent` и подхватываются на `idle`/`done`.
- **Стоп follower-крона** — `followerActive` снимается на `idle`/`done`; крон-фолловер
  перестаёт пинать сессию (проверяет `/state`).
- **Идемпотентность** — повтор мутации с тем же `Idempotency-Key` возвращает
  кэшированный ответ без повторного эффекта (TTL 24 ч, уборка по alarm).
- **Защита от гонок reconnect** — колбэки со stale `runId` отбиваются 409.

## Масштаб и лимиты
- 20 репо × 20 jobs = ~400 тёплых воркеров на аккаунт.
- Больше — добавляй аккаунты/организации, каждый со своим лимитом 20.
- Следи за fair-use GitHub: это для коротких batch-нагрузок, не для 24/7 майнинга.
