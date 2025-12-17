# Notebooks

- `weather_mart_insights.ipynb` — пример анализа витрины `dm.dm_weather_daily`.

## Как запустить

1) Поднять инфраструктуру и построить витрину:

```bash
docker compose up -d --build
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt deps && dbt build"
```

2) Открыть ноутбук и выполнить ячейки.

## Экспорт в HTML (для сдачи)

```bash
jupyter nbconvert --to html notebooks/weather_mart_insights.ipynb --output notebooks/weather_mart_insights.html
```
