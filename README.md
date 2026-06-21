## Geo Git — система версионирования геопространственных данных

### Описание

Geo Git — это система контроля версий для геопространственных данных
Основные сущности:
- **БПО (базовый пространственный объект)** — геометрия + атрибуты (GeoJSON)
- **Обстановка (situation)** — набор БПО, логически объединённых
- **Версия обстановки** — снимок состояния набора БПО с метаданными (автор, время, родительские версии)

Хранилище данных — MongoDB c коллекциями:
- `bpo_cas` — Content-Addressed Storage для БПО (объекты по хешу содержимого);
- `situations` — описания обстановок;
- `situation_versions` — версии обстановок;
- `version_deltas` — метаданные дельт между версиями (delta_id, from/to);
- `delta_items` — содержимое дельт (added/removed/modified/unchanged/likely_modified), по одному документу
  на запись, чтобы не упираться в лимит 16MB на документ при больших диффах.

### Архитектура

- `src/main.cpp` — точка входа, проверяет подключение к MongoDB, инициализацию БД и индексов.
- `src/storage/mongodb_connection/` — подключение к MongoDB:
  - создание `mongocxx::client`;
  - доступ к коллекциям (`bpo_cas`, `situations`, `situation_versions`, `version_deltas`);
  - проверка и инициализация индексов.
- `src/storage/bpo_storage/` — модель БПО и валидация GeoJSON:
  - `BPO` — оболочка над документом MongoDB (геометрия + атрибуты);
  - `GeoJSONValidator` — базовая проверка структуры GeoJSON (Point / LineString / Polygon).
- `src/schemas/init_mongodb.js` — скрипт инициализации БД (создание коллекций и геоиндексов).

### Сборка и запуск (Docker, рекомендуемый способ)

Проект состоит из трёх собираемых образов: `app` (демо-приложение), `grpc`
(gRPC-сервер) и `cli` (git-подобный клиент `geogit`), плюс `tests`. Подробности
сборки тестов — в [BUILD.md](BUILD.md), пошаговый сценарий работы с CLI и своим
GeoJSON — в [beta_test.md](beta_test.md).

```bash
# собрать образы
docker compose build grpc cli

# поднять MongoDB и gRPC-сервер
docker compose up -d mongodb grpc

# CLI — отдельная команда на каждый вызов
docker compose run --rm cli help
```

### Основные команды CLI (`geogit`)

| Команда | Назначение |
|---|---|
| `init <name> [description]` | создать обстановку (situation) и ветку `main` |
| `import <situation_id> <file.geojson> [-m msg] [-a author] [-b branch_id] [--gen-ids]` | импортировать GeoJSON как новую версию |
| `export <version_id> [file.geojson]` | выгрузить версию обратно в GeoJSON (в файл или stdout) |
| `log <situation_id>` | история версий обстановки |
| `show <version_id>` | метаданные и состав версии |
| `diff <v1> <v2> [--er]` | различия между версиями (`--er` — geo-сопоставление Level 2) |
| `status <branch_id> obj_id=hash ...` | сравнить набор объектов с HEAD ветки |
| `branch list\|create <situation_id> [name] [from_version_id]` | список веток / создание новой |
| `merge <base_v> <ours_v> <theirs_v> [--theirs]` | трёхстороннее слияние (`--theirs` — авторазрешение конфликтов) |
| `store-bpo <geometry_json> <attributes_json>` | положить один БПО в CAS, вернуть хеш |
| `get-bpo <hash>` | получить БПО из CAS по хешу |

Полный список и флаги — `docker compose run --rm cli help`.

### Сборка и запуск (локально, без Docker)

Нужны установленные mongo-c-driver / mongo-cxx-driver, OpenSSL, Boost (см.
`Dockerfile` для точных версий).

```bash
mongosh "mongodb://localhost:27017" < src/schemas/init_mongodb.js

mkdir -p build && cd build
cmake .. && make

./geoversion                                       # mongodb://localhost:27017 по умолчанию
./geoversion "mongodb://user:password@host:27017"  # или с явным URI
```

### Тесты

```bash
docker compose build tests
docker compose run --rm tests
```

### Автор: 
- Никоненко Егор
