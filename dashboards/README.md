# Superset Dashboard Export

`superset_export/` enthält den Export des Sales-Dashboards aus Apache Superset
(Dashboard, Charts, Dataset- und Datenbank-Metadaten als YAML). Passwörter sind
im Export maskiert.

## Re-Import

```bash
superset import-dashboards -p dashboards/superset_export.zip -u admin
```

oder in der Superset-UI unter *Dashboards → Import*.
