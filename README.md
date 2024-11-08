# railbe


[Belgische spoorwegdata ontsluiten](https://docs.irail.be/) via reguliere Python-code, naar een PostgreSQL-database (code check door Jasper)

- Stations-endpoint: https://api.irail.be/v1/stations/?format=json&lang=nl
- Vervolgens de vertrekborden ophalen: https://api.irail.be/liveboard/?id=BE.NMBS.008892007&lang=nl&format=json
    - *Deze haal je natuurlijk op voor elk station, dus de id-code verwijst naar het stations-ID*
- **Nieuw punt:**
    - Gebruik DuckDB (zie [DuckDB In Action](https://www.notion.so/DuckDB-In-Action-1347017c398580629eabdcc8e0019a3a?pvs=21) hoofdstuk 1, 2, 5, 6)
    - Sla de JSON op als .parquet files
    - Denk na over een retention policy: hoe lang sla je de Parquet-files op? (dit wordt m.n. relevant zodra je de data regelmatig, bijv. via Airflow, op gaat halen)
