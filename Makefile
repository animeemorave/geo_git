.PHONY: build test run clean docker-build docker-test docker-run

build:
	mkdir -p build
	cd build && cmake .. && make

test: build
	cd build && ctest

run: build
	./build/geoversion

clean:
	rm -rf build

docker-build:
	docker-compose build app

docker-test:
	docker-compose build tests
	docker-compose run --rm tests

docker-run:
	docker-compose up -d mongodb
	sleep 5
	docker-compose run --rm app

docker-stop:
	docker-compose down

docker-clean:
	docker-compose down -v
	docker system prune -f
