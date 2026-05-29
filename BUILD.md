# Сборка и запуск тестов

Все команды выполняются из корня проекта (`geo_git/`).

## Сборка образов

```bash
# Основное приложение
docker compose build app

# Образ с тестами
docker compose build tests
```

> Первая сборка долгая — внутри из исходников компилируются mongo-c-driver и
> mongo-cxx-driver. Эти слои кешируются, последующие сборки быстрые.
>
> Если сборка падает с `TLS handshake timeout` или `502 Bad Gateway` при загрузке
> метаданных `ubuntu:22.04` / при `apt-get` — это разовый сетевой сбой к Docker Hub
> или зеркалам Ubuntu. Просто повтори команду.

## Запуск тестов

### Способ 1 — через docker compose (просто, сам ждёт mongodb)

```bash
docker compose build tests
docker compose run --rm tests
```

`depends_on` дождётся, пока mongodb станет healthy, поднимет его и прогонит `ctest`.
`--rm` удалит контейнер после прогона.

### Способ 2 — явный (надёжнее, если compose лезет в пересборку)

```bash
# 1. Собрать образ
docker compose build tests

# 2. Поднять mongodb и дождаться готовности
docker compose up -d mongodb
until [ "$(docker inspect -f '{{.State.Health.Status}}' geoversion_mongodb)" = "healthy" ]; do sleep 2; done

# 3. Прогнать тесты на сети compose
docker run --rm \
  --network geo_git_geoversion_network \
  -e MONGODB_URI=mongodb://mongodb:27017 \
  geo_git-tests:latest
```

- `geo_git-tests:latest` — имя образа (производится от имени папки `geo_git`).
- `geo_git_geoversion_network` — сеть из `docker-compose.yml`.

## Полезные команды

```bash
# Список доступных тестов
docker run --rm geo_git-tests:latest ctest -N

# Запустить только один тест-модуль
docker run --rm --network geo_git_geoversion_network \
  -e MONGODB_URI=mongodb://mongodb:27017 \
  geo_git-tests:latest ctest -R test_situation_storage --output-on-failure

# Подробный вывод (видны все assert'ы)
docker run --rm --network geo_git_geoversion_network \
  -e MONGODB_URI=mongodb://mongodb:27017 \
  geo_git-tests:latest ctest --output-on-failure --verbose

# Остановить mongodb после работы
docker compose down

# Остановить + удалить данные mongodb (чистый старт)
docker compose down -v
```

## Ожидаемый результат

```
1/2 Test #1: test_cas .........................   Passed
2/2 Test #2: test_situation_storage ...........   Passed
100% tests passed, 0 tests failed out of 2
```

## Как устроены тесты

- Каждый модуль тестов — отдельный исполняемый файл с собственным `main()`
  (`tests/test_cas.cpp`, `tests/test_situation_storage.cpp`).
- Сборка тестов описана в `CMakeLists_test.txt`: общие исходники собираются в
  статическую библиотеку `geoversion_core`, на каждый тест-модуль создаётся
  отдельный бинарник и регистрируется через `add_test` (ctest).
- `Dockerfile.test` копирует `CMakeLists_test.txt` как `CMakeLists.txt`, собирает
  и запускает `ctest --output-on-failure`.
- Адрес MongoDB берётся из переменной окружения `MONGODB_URI`
  (по умолчанию `mongodb://mongodb:27017`).

### Добавление нового тест-модуля

1. Создать `tests/test_<имя>.cpp` с функцией `int main()`.
2. Добавить `test_<имя>` в список `TEST_MODULES` в `CMakeLists_test.txt`.
3. Пересобрать образ и прогнать тесты.
