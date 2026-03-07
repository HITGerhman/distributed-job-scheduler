SHELL := /usr/bin/env bash

.PHONY: up down ps logs health unit-test verify test demo demo-failover

up:
	docker compose -f deploy/docker-compose.yml up -d mysql mongo etcd
	./scripts/init_schema.sh
	docker compose -f deploy/docker-compose.yml up -d master-1 master-2 master-3 worker
	./scripts/healthcheck.sh

down:
	docker compose -f deploy/docker-compose.yml down

ps:
	docker compose -f deploy/docker-compose.yml ps

logs:
	docker compose -f deploy/docker-compose.yml logs --tail=100 -f

health:
	./scripts/healthcheck.sh

unit-test:
	go test ./...

verify:
	$(MAKE) up
	./scripts/verify_milestone8.sh

test:
	$(MAKE) up
	go test ./...
	./scripts/verify_milestone8.sh

demo:
	$(MAKE) up
	./scripts/demo.sh full

demo-failover:
	$(MAKE) up
	./scripts/demo.sh failover
