# API Programming (Programmazione di Sistema)

Benvenuto nella sezione **API Programming** del corso di **Programmazione di Sistema** presso il **Politecnico di Torino**.

Questo repository raccoglie tutto il materiale didattico, teorico e pratico necessario per superare l'esame: la guida completa allo studio (*Study Guide*), tutti i temi d'esame ufficiali risolti e verificati dal 2021 al 2026, 55 simulazioni avanzate di concorrenza e sincronizzazione in Rust, e i laboratori del corso.

---

## 🚀 Download Rapidi in 1-Click

Scarica direttamente gli elaborati completi con un singolo click:

| Risorsa | Descrizione | Link Download 1-Click | Dimensione |
| :--- | :--- | :---: | :---: |
| 📚 **Complete Study Guide** | Compendio teorico e pratico completo (24 capitoli, codice, layout di memoria) | [⬇️ **Scarica Study Guide (PDF)**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/STUDY_GUIDE/main.pdf) | ~11.5 MB |
| 📦 **Tutti gli Esami (2021–2026)** | Archivio ZIP contenente tutti i testi ufficiali e le soluzioni d'esame in PDF | [⬇️ **Scarica Tutti gli Esami (ZIP)**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/all_exams.zip) | ~5.5 MB |
| 🧪 **55 Simulazioni d'Esame** | Suite di 55 simulazioni Rust con test concorrenti e matrice di tracciabilità | [📁 **Esplora Directory Simulazioni**](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/SIMULATIONS) | ~5.4 MB |

---

## 🏛️ Struttura del Repository

La cartella `API PROGRAMMING` è organizzata in quattro macro-sezioni modulari:

```text
API PROGRAMMING/
├── STUDY_GUIDE/                  # Compendio teorico-pratico del corso
│   ├── main.pdf                  # PDF compilato completo della Study Guide (11.5 MB)
│   ├── main.tex                  # Sorgente LaTeX principale
│   ├── chapters/                 # 24 capitoli modulari (Ownership, Concorrenza, Canali, Tokio, ecc.)
│   └── images/                   # Diagrammi di memoria, architettura e grafi
│
├── EXAMS/                        # Archivio storico completo degli esami (2021–2026)
│   ├── all_exams.zip             # Pacchetto ZIP di tutti i PDF per download 1-click
│   ├── 2021/ ... 2026/           # Cartelle annuali con tutti gli appelli
│   └── questions_by_chapter.pdf  # Raccolta domande teoriche catalogate per argomento
│
├── SIMULATIONS/                  # Banco di 55 simulazioni concorsuali ad alta complessità
│   ├── SIMULATION01/ ... /55/    # Progetti Cargo con implementazione e test concorrenti
│   ├── exam_reverse_engineering.md # Studio analitico dei pattern d'esame e scoring rubric
│   └── traceability_matrix.md    # Matrice di copertura al 100% dei topic di corso
│
└── LABS/                         # Laboratori didattici del corso
    ├── LAB01/ ... /LAB07/        # Esercitazioni pratiche guidate
    └── README.md
```

---

## 📑 1. Study Guide (`STUDY_GUIDE/`)

La **Study Guide** ([`main.pdf`](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/STUDY_GUIDE/main.pdf)) rappresenta il manuale di riferimento completo per l'esame. È redatta in LaTeX ed è strutturata in capitoli tematici approfonditi:

* **Fondamenti del linguaggio**: Ownership, Borrowing, Lifetimes avanzati, Variance e Sotto-tipizzazione.
* **Error Handling & Tipi Composti**: Pattern matching, `Result<T, E>`, `Option<T>`, Custom Errors e conversioni (`From`/`Into`).
* **Programmazione Funzionale**: Chiusure (`Fn`, `FnMut`, `FnOnce`), cattura dell'ambiente e keyword `move`.
* **Iteratori & Pigrizia**: Adattatori (`map`, `filter`, `take`), metodi consumatori (`collect`, `sum`, `find`) e cortocircuitazione.
* **Puntatori Intelligenti & Grafi**: `Box`, `Rc`, `Arc`, `RefCell` (mutabilità interna e borrow check a runtime), prevenzione dei cicli di memoria con `Weak`.
* **Concorrenza Nativa**: `std::thread`, `Mutex`, `RwLock`, `Condvar`, atomici (`AtomicUsize`, `AtomicBool`, `Ordering`), poisoning e recupero.
* **Comunicazione a Messaggi**: Canali asincroni (`channel`) vs sincroni (`sync_channel`), saturazione dei buffer, backpressure e semantica di terminazione (`drop(tx)`).
* **Asincronia & Tokio**: `async`/`await`, Future, cooperative multitasking, task spawning, sleep e costrutti di unione (`join!`, `select!`).
* **Sistemi & FFI**: Chiamate a librerie C, ABI C, puntatori grezzi (`*const T`, `*mut T`) e blocchi `unsafe`.

---

## 🎓 2. Tabella Ufficiale Esami con Download 1-Click (2021–2026)

Tutti i 24 appelli d'esame sono completi di testo ufficiale del compito, soluzione teorica e implementazione pratica verificata con suite di unit test:

| Sessione | Data Appello | Argomenti Principali (Teoria & Pratica) | Download PDF (1-Click) | Codice Sorgente |
| :---: | :---: | :--- | :---: | :---: |
| **2026** | **08/09/2026** | Iteratori & Pigrizia, `Rc`/`RefCell`/`Weak`, `sync_channel` & Backpressure, **Rate Limiter** | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2026/exam_2026-09-08/exam_2026-09-08.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2026/exam_2026-09-08) |
| **2026** | **03/07/2026** | Lifetimes & NLL, Pipeline di canali MPSC, Tokio Async `task`, **Resource Pool** | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2026/exam_2026-07-03/exam_2026-07-03.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2026/exam_2026-07-03) |
| **2026** | **15/06/2026** | RAII & Scope, Mutex Poisoning & Panic, Chiusure `Fn`/`FnMut`/`FnOnce`, **Forgettable Channel** | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2026/2026-06-15/exam_2026-06-15.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2026/2026-06-15) |
| **2025** | **15/09/2025** | Subtyping & Variance, Thread pool dispatching, Condvar rendezvous | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-09-15/exam_2025-09-15.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-09-15) |
| **2025** | **01/09/2025** | Trait objects (`dyn`), Object Safety, Canali multipli coordinati | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-09-01/exam_2025-09-01.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-09-01) |
| **2025** | **03/07/2025** | Mutabilità interna (`Cell`/`RefCell`), Dynamic dispatch, Deadlock avoidance | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-07-03/exam_2025-07-03.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-07-03) |
| **2025** | **17/06/2025** | Lifetimes anonimi e associati, Tokio runtime scheduling, Bounded buffer | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-06-17/exam_2025-06-17.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-06-17) |
| **2025** | **13/01/2025** | Deref coercion, Smart pointer compositi, Gestione broadcast asincrono | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-01-13/exam_2025-01-13.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2025/exam_2025-01-13) |
| **2024** | **11/07/2024** | Trait bounds avanzati, Atomic reference counter, Read-Write locks | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2024/exam_2024-07-11/exam_2024-07-11.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2024/exam_2024-07-11) |
| **2024** | **25/06/2024** | Chiusure mutabili concorrenti, Drop ordering LIFO, MPSC sync channels | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2024/exam_2024-06-25/exam_2024-06-25.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2024/exam_2024-06-25) |
| **2024** | **22/01/2024** | Pinning, Future manuali, Arc & Weak graph traversal | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2024/exam_2024-01-22/exam_2024-01-22.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2024/exam_2024-01-22) |
| **2023** | **04/09/2023** | Lifetimes elision rules, Canali asincroni con timeout, Barrier synchronization | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2023/exam_2023-09-04/exam_2023-09-04.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2023/exam_2023-09-04) |
| **2023** | **07/07/2023** | Send and Sync safety invariants, Shared memory multi-threading | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2023/exam_2023-07-07/exam_2023-07-07.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2023/exam_2023-07-07) |
| **2023** | **20/06/2023** | Iterator adapters pipeline, Thread starvation avoidance, Condition variables | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2023/exam_2023-06-20/exam_2023-06-20.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2023/exam_2023-06-20) |
| **2023** | **16/01/2023** | Custom Allocators/Drop, Mutex poisoning recovery, Pipeline di stream | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2023/exam_2023-01-16/exam_2023-01-16.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2023/exam_2023-01-16) |
| **2022** | **26/10/2022** | Cyclic reference graphs, Unsafe cell semantics, Mutex & condvar ring buffer | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2022/exam_2022-10-26/exam_2022-10-26.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2022/exam_2022-10-26) |
| **2022** | **08/09/2022** | Tokio cooperative scheduling, Channel shutdown signaling, RAII guards | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2022/exam_2022-09-08/exam_solution_2022-09-08.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2022/exam_2022-09-08) |
| **2022** | **08/07/2022** | Arc interior mutability, Atomic operations (`Ordering::SeqCst`), Thread pools | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2022/exam_2022-07-08/exam_2022-07-08.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2022/exam_2022-07-08) |
| **2022** | **20/06/2022** | Generics monomorphization, Zero-cost abstractions, Backpressure | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2022/exam_2022-06-20/exam_2022-06-20.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2022/exam_2022-06-20) |
| **2021** | **18/10/2021** | Borrowing across thread boundaries, Multi-producer sync queue | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-10-18/exam_2021-10-18.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-10-18) |
| **2021** | **02/09/2021** | Weak pointer upgrade semantics, Drop flag analysis, Deadlock detection | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-09-02/exam_2021-09-02.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-09-02) |
| **2021** | **05/07/2021** | Trait bounds static dispatch, Arc vs Rc threading guarantees, Channels | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-07-05/exam_2021-07-05.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-07-05) |
| **2021** | **09/06/2021** | Mutex vs RwLock read-heavy workloads, Thread joining invariants | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-06-09/exam_2021-06-09.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-06-09) |
| **2021** | **15/02/2021** | Rust ownership model basics, Primitive types, Initial thread spawning | [📄 **Scarica PDF**](https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-02-15/exam_2021-02-15.pdf) | [📁 Vai al Codice](https://github.com/Beccaceci/PDS/tree/main/API%20PROGRAMMING/EXAMS/2021/exam_2021-02-15) |

---

## 💻 Download Rapido da Terminale (CLI)

Se preferisci scaricare i materiali direttamente da riga di comando (macOS / Linux):

### 1. Scarica la Study Guide completa
```bash
curl -fSL -o "Study_Guide_Rust.pdf" \
  "https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/STUDY_GUIDE/main.pdf"
```

### 2. Scarica tutti gli esami in un unico archivio ZIP
```bash
curl -fSL -o "PDS_Exams_2021_2026.zip" \
  "https://github.com/Beccaceci/PDS/raw/main/API%20PROGRAMMING/EXAMS/all_exams.zip"
unzip PDS_Exams_2021_2026.zip -d PDS_Exams/
```

---

## 🎯 Criteri di Valutazione dell'Esame

La prova d'esame è strutturata su **15,0 punti grezzi**, convertiti in trentesimi tramite moltiplicatore $\times 2$:

$$\text{Voto Finale} = (\text{Punti Teoria} + \text{Punti Pratica}) \times 2$$

### 1. Parte Teorica (Prof. Maurizio Rebaudengo) --- 9,0 punti (60%)
Composta da 3 esercizi da 3,0 punti ciascuno suddivisi in sotto-quesiti (con frazioni da 0,25 a 1,5 pt):
* **Parole chiave fondamentali**: *pigrizia degli iteratori*, *adattatore vs consumatore*, *cortocircuito*, *mutabilità interna*, *spostamento del borrow check a runtime*, *cicli di riferimenti forti e mancata deallocazione*, *backpressure*, *saturazione buffer sincrono*, *chiusura canale tramite drop di tutti i trasmettitori*.
* **Precisione dell'output**: Le domande con tracciamento richiedono l'ordine esatto delle righe e la corretta formattazione degli enum (`Some(...)`, `None`, stampe dei distruttori `drop`).

### 2. Parte Pratica (Prof. Luca Malnati) --- 6,0 punti (40%)
Richiede la progettazione e implementazione di una struttura dati concorrente in Rust:
* **Vincoli tassativi**: 
  - Il codice **deve compilare**: errori di compilazione comportano l'annullamento della prova pratica.
  - **Assenza di attesa attiva (No busy-waiting)**: sospendere i thread con `Condvar` o `std::thread::sleep(duration)` rilasciando il lock.
  - **Nessuna sincronizzazione ridondante**: evitare `Arc` superflui all'interno della struct se il metodo riceve `&self` o `Condvar` non necessarie quando lo sblocco dipende solo dal tempo.
  - **Thread-Safety & Correttezza Concorrente**: rispetto di `Send` e `Sync`, assenza di deadlock e gestione di risvegli spuri mediante costrutti `loop { ... }`.
