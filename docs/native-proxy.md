# CMMS native proxy (AF_UNIX)

Локальный транспорт CMMS: один процесс-владелец держит стор и единственный
AF_UNIX сокет, клиенты (Hermes-плагин `memory_server_proxy`) ходят в него
короткими сессиями. MCP stdio остаётся отдельным транспортом и с native-режимом
на одном `data_root` не совмещается.

## Архитектура

Три роли, три разных процесса:

- **owner** — процесс `native-serve`. Валидирует service JSON, провижинит
  `store_id`, берёт root-lock на `data_root`, слушает AF_UNIX сокет (mode 0600
  внутри каталога 0700), поднимает backend (`HermesProvider`) и обслуживает
  запросы. Ровно один owner на один `data_root`.
- **service** — сам сервисный цикл: список состояний `STARTING → READY →
  CLOSING → CLOSED`, отдельно `FAILED_CLOSING`. В `READY` на каждое соединение
  проверяется peer credential (`SO_PEERCRED`), затем принимается hello-кадр и
  один запрос.
- **client** — плагин `memory_server_proxy`: `connect → hello → op → response →
  close` на каждый вызов инструмента. Плагин stdlib-only, backend не импортирует,
  знает только путь к сокету и ожидаемый `store_id` из sidecar.

Кадр wire v1: 4-байтовый big-endian заголовок длины + JSON-объект. Hello
содержит `v`, `op="hello"`, `correlation_id`, `schema_hash`, `store_id`,
`claimed_home`; ответ содержит `state`, `store_id`, `epoch`, `schema_hash`.
Readiness = `state == "READY"` **и** совпадение `store_id` **и** совпадение
`schema_hash` с канонической схемой v1.

## Service JSON

Один операторский файл, mode 0600, владелец — тот же UID, что у сервиса.
Полный валидный пример: `docs/examples/native-service.json`.

Обязательные ключи: `version` (строго `1`), `data_root`,
`storage_context_home`, `socket_path`, `allow_homes`, `backend`.
Опциональные: `store_id` (канонический UUIDv4; провижинится один раз),
`limits`.

Ограничения схемы:

- `data_root` и `storage_context_home` — абсолютные пути, `data_root` не `/` и не
  домашний каталог; `db_url`/`lancedb_path`/`graph_snapshot_path` обязаны лежать
  под `data_root`;
- `socket_path` — абсолютный, ≤ 107 байт, родительский каталог существует, не
  symlink, mode 0700, владелец — euid сервиса;
- `allow_homes` — непустой список существующих абсолютных каталогов; клиент с
  `claimed_home` вне списка получает `E_PROFILE_DENIED`;
- `backend.storage_mode` — только `shared`; `vector_backend` — `lancedb`,
  `qdrant` или `none`; `extraction_mode` — `llm` или `none`.

Секретов в файле нет: креды экстрактора передаются только через
`EnvironmentFile=` systemd (0600).

## Команды CLI

```
memory-server native-provision --config /srv/cmms/service.json
memory-server native-serve     --config /srv/cmms/service.json
memory-server native-status    --config /srv/cmms/service.json
memory-server install-hermes-proxy --hermes-home "$HERMES_HOME" \
    --socket /run/cmms-native/memory.sock \
    --expected-store-id <uuid> [--activate] [--dry-run] [--uninstall]
```

Те же команды доступны как `python -m memory_server.native.cli` (этим пользуется
`scripts/install-proxy-plugin.sh`).

- `native-provision` печатает `{"path": ..., "store_id": ...}`; повторный запуск
  ничего не меняет и печатает тот же `store_id`.
- `native-serve` — блокирующий цикл сервиса; останавливается по SIGTERM/SIGINT.
- `native-status` делает hello и печатает `state`/`store_id`/`epoch`/`schema_hash`.
- `install-hermes-proxy` копирует `plugin-dist/memory_server_proxy/` в
  `$HERMES_HOME/plugins/memory_server_proxy/` (пофайлово, без `__pycache__`),
  пишет sidecar `$HERMES_HOME/memory_server_proxy.json` (0600, атомарно) и
  печатает JSON-результат. `--activate` дополнительно ставит
  `memory.provider: memory_server_proxy` в `config.yaml` (перед этим —
  `config.yaml.bak-<UTC>`). `--dry-run` печатает план (`would_write`) и не пишет
  ничего. `--uninstall` удаляет только `plugins/memory_server_proxy/` и sidecar.

## Коды выхода

| код | значение |
|-----|----------|
| 0 | успех |
| 2 | ошибка конфигурации или сервис недоступен (`E_CONFIG_*`, `E_STORE_ID`, `E_LAYOUT_MISMATCH`, `E_UNAVAILABLE`, отказ hello) |
| 3 | `FAILED_CLOSING`: shutdown не закрыл lock/сокет |
| 4 | путь сокета занят (`E_STALE_SOCKET`, `FileExistsError`) |
| 5 | второй владелец (`E_LOCK`, `StorageOwnershipError`) |

## Runbook

Запуск и остановка (systemd; пример юнита — `docs/examples/memory-server-native.service`):

```
systemctl start memory-server-native
systemctl status memory-server-native
memory-server native-status --config /srv/cmms/service.json
systemctl stop memory-server-native
```

Проверка «жив ли» и совпадает ли он с конфигом: `native-status`. Код 0 — сервис
`READY`, `store_id` и `schema_hash` совпадают. Код 2 — недоступен или конфиг не
совпадает (текст кода в stderr). Код 3 — предыдущая остановка была грязной.

Stale socket (код 4, в stderr `E_STALE_SOCKET`):

1. убедиться, что процесс сервиса не запущен (`systemctl status`,
   `pgrep -f "native-serve"`);
2. удалить **только** файл сокета из `socket_path` (каталог 0700, владелец —
   сервисный UID);
3. запустить сервис заново и проверить `native-status`.

FAILED_CLOSING (код 3):

1. `RestartPreventExitStatus=3` не даёт systemd крутить рестарты;
2. посмотреть лог сервиса — код причины в отчёте shutdown
   (`E_RUNTIME_CLEANUP_FAILED`, `E_RUNTIME_FLUSH_FAILED`, удержанный lock);
3. убедиться, что lock `data_root/.cmms-storage.lock` не удерживается другим
   процессом;
4. устранить причину и запустить сервис; сокет при FAILED_CLOSING сохраняется
   намеренно — см. пункт про stale socket.

Второй владелец (код 5): уже есть процесс на этом `data_root`. Остановить его,
дождаться снятия lock, повторить запуск. Параллельный `native-serve` и MCP stdio
на одном `data_root` запрещён.

## Ограничения

- **same-UID**: сервис и клиенты обязаны работать под одним UID; разделения на
  отдельный сервисный UID в этом контракте нет.
- **Один `data_root` — один сервис**: второй `native-serve` на тот же стор
  завершается кодом 5.
- **MCP stdio несовместим** с одновременным native-сервисом на том же
  `data_root` (конкурентная запись).
- **SIGKILL** (и падение OOM) не даёт выполнить drain: теряется RAM-очередь
  writer'а и остаётся stale socket — ручная очистка по runbook.
- Клиент и сервис должны иметь одинаковую каноническую схему v1, иначе hello
  отклоняется с `E_SCHEMA`.
- `native-status` не заменяет health-check самого Hermes: он проверяет только
  канал и идентичность сервиса.

## Owner-решения O1–O10

| # | Решение |
|---|---------|
| O1 | Сервис и клиенты — под одним UID; отдельный сервисный UID не вводится (двусторонний peercred + parent 0700) |
| O2 | Каталог сокета 0700, `data_root` и `storage_context_home` 0700, `umask 077` при provisioning |
| O3 | `allow_homes` — явный список домашних каталогов профилей; изменение только правкой service JSON и рестартом; пустой список запрещён |
| O4 | Креды экстрактора — только через systemd `EnvironmentFile=` (0600), не в service JSON и не в конфиге профиля |
| O5 | `llm_base_url` — текущий провайдер, `extraction_mode=llm`; при отказе от egress — `none` |
| O6 | SIGTERM → pre-drain 8 с → `runtime.close` (≤30 с); `TimeoutStopSec=45`; `RestartPreventExitStatus=3`; SIGKILL = потеря RAM-очереди + stale socket |
| O7 | Один `data_root` — один сервис; параллельный MCP stdio запрещён и документируется |
| O8 | `data_root` вне home; sandbox systemd (NoNewPrivileges, PrivateTmp, ProtectSystem=strict, ProtectHome=read-only, ReadWritePaths, RestrictAddressFamilies=AF_UNIX); логи 0600 |
| O9 | Каталог плагина и sidecar принадлежат клиентскому UID, 0700/0600; проверяется sha256-манифест `plugin-dist`; чужой UID не имеет права записи |
| O10 | Provenance best-effort: `source`/`session_id` информационные, не аутентификация |

## Безопасность

- **Права**: service JSON 0600, каталог сокета 0700, сокет 0600, sidecar 0600,
  каталог плагина 0700, логи 0600. Все они принадлежат одному UID.
- **Peercred**: на каждом соединении ядро сообщает UID пира (`SO_PEERCRED`);
  чужой UID получает `E_PROFILE_DENIED` до чтения кадра. Это admission, а не
  аутентификация: same-UID процессы доверяют друг другу.
- **Атрибуция**: `source`, `session_id` и `claimed_home` контролируются
  клиентом и не служат доказательством авторства (O10).
- **Логи**: пишутся под сервисным UID с mode 0600; в них нет содержимого фактов,
  ключей и URL с креденшелами. Ограничения записи — `ProtectSystem`,
  `PrivateTmp`, `ReadWritePaths` (O8).
- **Секреты**: в service JSON, sidecar и документации секретов быть не должно;
  креды — только `EnvironmentFile=`.
