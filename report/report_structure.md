# Struttura del report
## Presentazione
* Contesto - Parlare della piattaforma youtube, diffusione, fonte di guadagno e usato per scopi promozionali
* Presenza di tecniche per ATTIRARE L'ATTENZIONE, invogliare al click (portare esempi)
* **Scopo** cercare di determinare queste tecniche attraverso i big data.

## Descrizione dei dataset
Per ognuno scrivere:
* Da dove viene
* Cosa contiene, evidenziare la struttura (parlare del fatto che il file di kaggle è trattato come un database relazionale)
* Per quale motivo abbiamo scelto di includerlo

## Descrizione dell'architettura
* well, just that

## Data loading
* Presentazione degli script con le relative analisi di performance
* Attenzione ad una prima fase di schema transformation in dataset kaggle - Approccio full embedded fallito e sostituito

## Assessment della qualità iniziale dei dati
* Il problema dell'accuratezza
* L'affidabilità degli ID nel dataset kaggle e nel dataset API
* La completezza nei 3 dataset
* La consistenza nel dataset kaggle (al variare della data), non si può sapere se il titolo è sbagliato o contiene degli errori, stessa cosa per il canale ecc.. Invece non vale per la data di caricamento.

## Data enrichment
* Obiettivo della procedura - Aumento della completezza nel dataset
* Descrizione dei 3 step
* Test successivo della completezza dei dataset

## Data integration
* Descrizione dell'obiettivo
* Descrizione della scelta nella data fusion - Coerenza interna - Decisione di mantenere i dati incompleti ma coerenti internamente
* Descrizione della procedura con analisi di performance

## Data enrichment finale
* Descrizione dell'aggiunta dei campi con relativa analisi di performance

# Data exploration

# Data viz

# Conclusioni
