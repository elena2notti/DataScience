# Data Science Project – Cultural Heritage Databases

Questo progetto sviluppa un **software per l’integrazione e la consultazione di dati sul patrimonio culturale** provenienti da fonti eterogenee. L’applicazione permette di:  

- **Caricare** dati: in **JSON** per creare un database **relazionale** con informazioni sui processi di acquisizione e digitalizzazione; in **CSV** per creare un **graph database** (Blazegraph) con i metadati degli oggetti culturali.  
- **Interrogare simultaneamente** i due database tramite query predefinite (mashup).  

## Struttura principale
- **UploadHandler**: caricamento dati nei database.  
- **QueryHandler**: interrogazione dei dati (relazionale e graph).  
- **BasicMashup / AdvancedMashup**: query integrate su più database.  

## Requisiti
- Python 3.x  
- Blazegraph (per il graph database)  

## Esecuzione rapida
1. Avviare Blazegraph: `java -server -Xmx1g -jar blazegraph.jar`  
2. Creare i database e caricare i dati di esempio:  

```python
from impl import ProcessDataUploadHandler, MetadataUploadHandler

process = ProcessDataUploadHandler()
process.setDbPathOrUrl("relational.db")
process.pushDataToDb("data/process.json")

metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")
metadata.pushDataToDb("data/meta.csv")
```

3. Eseguire il test base: python -m unittest test
