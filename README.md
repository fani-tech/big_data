# Big Data Project (Spark)

Υλοποίηση εξαμηνιαίας εργασίας (Apache Spark / Hadoop).

## Γρήγορη εκκίνηση (τοπικά)

Προτεινόμενη Python: **3.10–3.12** (Spark 3.5 / PySpark).

### 1) Δημιουργία venv
```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
pip install -e .
```

> Σημείωση (Windows): αν δεις σφάλμα τύπου `socketserver.UnixStreamServer`/`AF_UNIX` στο import του PySpark,
> τότε το τρέχον Windows/Python περιβάλλον σου δεν υποστηρίζει Unix Domain Sockets.
> Πρακτική λύση για local δοκιμές είναι WSL2 (Ubuntu) ή Linux περιβάλλον.

### 2) Εκτέλεση σε local mode με sample δεδομένα
```bash
python -m bigdata_project.cli run-all --storage local --am <AM>
```

Αποθηκεύονται:
- Parquet στο `./results/parquet_v2/...`
- timings στο `./results/timings.json`
- explain outputs στο `./results/explain/...`
- εξατομίκευση (h,d,K) στο `./results/personalization.txt`

## Εκτέλεση στο εργαστήριο (HDFS/Kubernetes)

Το project είναι παραμετροποιήσιμο ώστε να τρέχει είτε τοπικά (local filesystem) είτε πάνω στο HDFS.

Για HDFS mode απαιτείται να γνωρίζετε το `{username}` στο HDFS **ή** να δώσετε ρητά output paths.

Παράδειγμα (ενδεικτικό):
```bash
python -m bigdata_project.cli run-all \
  --storage hdfs \
  --hdfs-user <USERNAME> \
  --am <AM>
```

> Σημείωση: οι ακριβείς οδηγίες σύνδεσης/εκτέλεσης στο Kubernetes (και η ρύθμιση Job History Server) πρέπει να ακολουθήσουν τους οδηγούς του μαθήματος.

## Δομή κώδικα
- `src/bigdata_project/` βασικό package
- `src/bigdata_project/queries/` υλοποιήσεις Q1–Q6
- `src/bigdata_project/benchmark.py` μετρήσεις χρόνων
- `src/bigdata_project/joins_study.py` μελέτη joins/optimizer (Μέρος 1Β)

## Οδηγοί
- `docs/howto/local.md` (local run + tests)
- `docs/howto/hdfs.md` (HDFS/Kubernetes runs + Q6 scaling)
- `docker/02-lab2-spark-history-server/` (επίσημο setup Spark History Server όπως στους οδηγούς)
- `docker/` (legacy skeleton — μην το προτιμάς για το lab)

