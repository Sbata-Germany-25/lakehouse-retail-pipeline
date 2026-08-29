# lakehouse-retail-pipeline
'''
mkdir -p ./logs ./plugins ./config
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.4/docker-compose.yaml'
'''
- genau richtig. mkdir -p ./logs ./plugins ./config erstellt drei Ordner im aktuellen Verzeichnis:

- logs/
- plugins/
- config/

- curl — lädt Inhalte von einer URL herunter
- L — folgt Weiterleitungen (falls die URL umgeleitet wird, z.B. auf eine andere Version)
- f — "fail silently": wenn der Server einen Fehler zurückgibt (z.B. 404, falsche Version), bricht curl ab statt eine Fehlerseite als Datei zu speichern
- O — speichert die heruntergeladene Datei unter ihrem Original-Dateinamen aus der URL (hier: docker-compose.yaml), statt sie z.B. im Terminal auszugeben.