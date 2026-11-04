ENGINE ?= duckdb
# Préfixe optionnel pour plafonner les commandes lourdes, par exemple :
#   make bench LANCEUR="systemd-run --user --scope -p CPUQuota=125% -p MemoryMax=5G"
LANCEUR ?=
PASSAGES ?= 2
PYTHON := $(LANCEUR) .venv/bin/python

# Spark 4.2 demande Java 17 ou 21. Si JAVA_HOME n'est pas défini, le code cherche
# un JDK compatible dans /usr/lib/jvm (voir spark_pipeline/session.py).
export JAVA_HOME ?= $(firstword $(wildcard /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/java-21-openjdk-amd64 /usr/lib/jvm/temurin-21-jdk-amd64))

.PHONY: help install download bronze silver gold pipeline quality compare bench figures test fixture all clean

help:
	@echo "make install                 venv Python 3.12 et dépendances épinglées (uv)"
	@echo "make download                fichiers géo-DVF dans data/raw (idempotent)"
	@echo "make bronze|silver|gold      une couche, ENGINE=duckdb (défaut) ou ENGINE=spark"
	@echo "make pipeline                bronze + silver + gold pour ENGINE"
	@echo "make quality                 contrôles qualité et rapport docs/qualite_ENGINE.md"
	@echo "make compare                 parité Spark / DuckDB sur le lac complet"
	@echo "make bench                   benchmark des deux moteurs, PASSAGES=2 (docs/benchmark.md)"
	@echo "make figures                 figures PNG dans docs/"
	@echo "make test                    tests pytest sur la fixture"
	@echo "make all                     tout, des téléchargements aux figures"

install:
	uv venv --python 3.12
	uv sync --frozen

download:
	$(PYTHON) -m dvf_lakehouse download

bronze silver gold pipeline:
	$(PYTHON) -m dvf_lakehouse $@ --engine $(ENGINE)

quality:
	$(PYTHON) -m dvf_lakehouse qualite --engine $(ENGINE)

compare:
	$(PYTHON) -m dvf_lakehouse comparer

bench:
	$(PYTHON) -m dvf_lakehouse bench --repetitions $(PASSAGES)

figures:
	$(PYTHON) -m dvf_lakehouse figures --engine $(ENGINE)

test:
	$(PYTHON) -m pytest

fixture:
	$(PYTHON) tests/fixtures/generer_fixture.py

all: download
	$(MAKE) bench
	$(MAKE) quality ENGINE=duckdb
	$(MAKE) quality ENGINE=spark
	$(MAKE) compare
	$(MAKE) figures

clean:
	rm -rf lake .tmp
