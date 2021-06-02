#!/usr/bin/env make

install:
	pip3 install -r requirements.txt

freeze:
	pip3 freeze > requirements.txt

venv:
	python3 -m venv venv

down:
	docker-compose down

up:
	docker-compose up -d
