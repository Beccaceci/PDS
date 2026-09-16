# OS Internals (Programmazione di Sistema)

Benvenuto nella sezione **OS Internals** del corso di **Programmazione di Sistema** presso il **Politecnico di Torino**.

Questa directory raccoglie il materiale completo relativo all'architettura e al funzionamento interno dei moderni sistemi operativi, con particolare riferimento al kernel didattico **OS/161**, alle chiamate di sistema, alla gestione della memoria virtuale (paginazione, TLB e swapping), alla sincronizzazione del kernel e a tutti i **temi d'esame ufficiali con risoluzioni dettagliate** dal 2020 al 2026.

---

## 🚀 Download Rapidi in 1-Click

Scarica direttamente le risorse chiave con un singolo click:

| Risorsa | Descrizione | Link Download 1-Click | Dimensione |
| :--- | :--- | :---: | :---: |
| 📚 **OS/161 Lab & Study Guide** | Guida completa e approfondita ai laboratori e all'architettura interna di OS/161 | [⬇️ **Scarica Study Guide (PDF)**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/LABS/study_guide/OS161_LABS.pdf) | ~4.47 MB |
| 📦 **Tutti gli Esami + Risoluzioni (2020–2026)** | Archivio ZIP contenente tutti i 48 file PDF (24 testi d'esame + 24 soluzioni ufficiali) | [⬇️ **Scarica Tutti gli Esami (ZIP)**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/all_exams.zip) | ~21.0 MB |

---

## 🏛️ Struttura del Repository

La sezione `OS INTERNALS` è articolata in due aree principali:

```text
OS INTERNALS/
├── README.md                         # Questo documento descrittivo e indice dei download
│
├── LABS/                             # Laboratori sul kernel didattico OS/161
│   └── study_guide/                  # Manuali e guide di studio
│       ├── OS161_LABS.pdf            # Guida completa compilata (4.47 MB)
│       └── .gitignore
│
└── EXAMS/                            # Archivio storico degli esami (2020–2026)
    ├── all_exams.zip                 # Pacchetto ZIP completo di tutti i PDF (21.0 MB)
    ├── 2020-2021/                    # Appelli 2020-06-27, 2020-07-10, 2020-09-12, 2021-02-15
    ├── 2021-2022/                    # Appelli 2021-06-09, 2021-07-05, 2021-09-02, 2021-10-18, 2022-01-17
    ├── 2022-2023/                    # Appelli 2022-06-20, 2022-07-08, 2022-09-08, 2022-10-26, 2023-01-16
    ├── 2023-2024/                    # Appelli 2023-06-20, 2023-07-07, 2023-09-04, 2024-01-22
    ├── 2024-2025/                    # Appelli 2024-06-25, 2024-07-11, 2024-09-02, 2025-01-13
    └── 2025-2026/                    # Appelli 2025-09-01, 2026-01-16
```

---

## 🔬 Fondamenti Teorici e Competenze Chiave di OS Internals

Il programma d'esame di OS Internals richiede la padronanza dei meccanismi a basso livello che regolano il sistema operativo:

### 1. Gestione dei Processi, Thread e Scheduling
* **Struttura del Processo**: Process Control Block (`struct proc`), thread di kernel (`struct thread`), stack utente vs stack di kernel.
* **Context Switch & Trapframe**: Meccanica del cambio di contesto hardware/software, salvataggio e ripristino dei registri della CPU (`trapframe`).
* **Algoritmi di Scheduling**: Round Robin, Priority Scheduling, Multi-Level Feedback Queues (MLFQ), prevenzione dell'inversione di priorità (*Priority Inheritance*).
* **Sincronizzazione di Kernel**: Semafori contatori, Spinlock (disabilitazione degli interrupt), Lock/Mutex con attesa passiva (*Wait Channels* - `wchan`).

### 2. Memoria Virtuale e Gestione degli Spazi d'Indirizzamento
* **Paginazione & Segmentazione**: Traduzione logico-fisica, calcolo di offset e page number, dimensione della pagina ($4\text{ KB}$).
* **Strutture Tabelle delle Pagine**: Tabelle a livello singolo, tabelle multilivello (radice, directory, leaf), tabelle invertite (*Inverted Page Tables* con hashing).
* **Gestione del TLB (Translation Lookaside Buffer)**: Meccanismo di TLB Hit e TLB Miss (software-managed in MIPS/OS161), replacement FIFO/LRU/Random, bit di validità, dirty bit e protezione.
* **Page Fault & Swapping**: Handler di page fault, allocazione di frame fisici (`coremap`), algoritmi di rimpiazzo (FIFO, Second Chance / Clock, LRU), gestione dello spazio di swap su disco.

### 3. Chiamate di Sistema (System Calls) e Transizione Utente/Kernel
* **Interfaccia Hardware/Kernel**: Istruzione `syscall`, commutazione da User Mode a Kernel Mode (bit di privilege level), passaggio parametri nei registri (`a0`–`a3`) e valori di ritorno (`v0`, `v1`).
* **Implementazione Syscall di Processo**: `sys_fork` (clonazione dello spazio d'indirizzamento e trapframe del figlio), `sys_execv` (sostituzione del codice eseguibile ed ELF loader), `sys_waitpid` (sincronizzazione padre-figlio con exit code), `sys__exit` (terminazione e rilascio risorse).
* **Implementazione Syscall File System**: `sys_open`, `sys_read`, `sys_write`, `sys_close`, `sys_lseek`, `sys_dup2`.

### 4. File System e Astrazione I/O
* **Gerarchia a Tre Livelli**: Per-Process File Descriptor Table (FDT) $\to$ System-wide Open File Table (OFT con file offset e reference count) $\to$ Virtual Node Table (`vnode` / `inode`).
* **Condivisione post-fork**: Meccanica di condivisione dell'OFT tra genitore e figlio dopo `fork()`, duplicazione dei descrittori con `dup2()`.

---

## 📑 2. Guida ai Laboratori OS/161 (`OS161_LABS.pdf`)

Il file [`OS161_LABS.pdf`](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/LABS/study_guide/OS161_LABS.pdf) (4.47 MB) copre l'intero percorso pratico di estensione del kernel OS/161:

* **LAB 1**: Configurazione dell'ambiente di simulazione `sys161`, compilazione del kernel, debugging con `gdb`, prima analisi delle strutture `thread` e `wchan`.
* **LAB 2 (Sincronizzazione)**: Implementazione dei Lock con mutua esclusione (`lock_create`, `lock_acquire`, `lock_release`), Condition Variables (`cv_wait`, `cv_signal`, `cv_broadcast`) e risoluzione del problema dei filosofi a cena.
* **LAB 3 (Chiamate di Sistema di Base)**: Implementazione di `open`, `read`, `write`, `close`, gestione della tabella dei file aperti (OFT) e dei descrittori di processo (FDT), supporto a `stdout` e `stdin`.
* **LAB 4 (Gestione Processi)**: Implementazione completa di `fork`, coordinazione padre-figlio tramite `waitpid`, gestione dell'exit code, riassegnazione dei processi orfani al processo `init`.
* **LAB 5 (Memoria Virtuale)**: Rimpiazzo di `dumbvm`, implementazione della `coremap` per l'allocazione dinamica dei frame fisici, gestione delle pagine on-demand e gestione del TLB miss per pagine di codice, dati e stack.

---

## 🎓 3. Tabella Master Esami con Download 1-Click (2020–2026)

L'archivio comprende tutti i **24 appelli d'esame** degli ultimi sei anni accademici. Ogni appello offre sia il **testo ufficiale del compito** sia la **risoluzione passo-passo**:

| A.A. | Data Appello | Argomenti Chiave (Teoria, Esercizi & OS/161) | Testo (1-Click) | Soluzione Ufficiale (1-Click) |
| :---: | :---: | :--- | :---: | :---: |
| **2025-2026** | **16/01/2026** | TLB Miss handler, Gestione multithreading kernel, `fork`/`execv` stack | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2025-2026/2026-01-16/exam_2026-01-16.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2025-2026/2026-01-16/solution_2026-01-16.pdf) |
| **2025-2026** | **01/09/2025** | Paginazione multilivello, Algoritmo Clock, `sys_waitpid` | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2025-2026/2025-09-01/exam_2025-09-01.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2025-2026/2025-09-01/solution_2025-09-01.pdf) |
| **2024-2025** | **13/01/2025** | Coremap physical memory management, Trapframe handling | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2024-2025/2025-01-13/exam_2025-01-13.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2024-2025/2025-01-13/solution_2025-01-13.pdf) |
| **2024-2025** | **02/09/2024** | Tabella dei file aperti, Condivisione descrittori post-fork, Deadlock | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2024-2025/2024-09-02/exam_2024-09-02.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2024-2025/2024-09-02/solution_2024-09-02.pdf) |
| **2024-2025** | **11/07/2024** | Paginazione a 2 livelli, Page Fault in dumbvm, Semafori | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2024-2025/2024-07-11/exam_2024-07-11.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2024-2025/2024-07-11/solution_2024-07-11.pdf) |
| **2024-2025** | **25/06/2024** | Context switch tra thread kernel, Scheduler a priorità, Inodes | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2024-2025/2024-06-25/exam_2024-06-25.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2024-2025/2024-06-25/solution_2024-06-25.pdf) |
| **2023-2024** | **22/01/2024** | Swapping su disco, Coremap entry states, Syscall `execv` arguments | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2023-2024/2024-01-22/exam_2024-01-22.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2023-2024/2024-01-22/solution_2024-01-22.pdf) |
| **2023-2024** | **04/09/2023** | Inverted Page Table, TLB replacement policies, File table offsets | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2023-2024/2023-09-04/exam_2023-09-04.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2023-2024/2023-09-04/solution_2023-09-04.pdf) |
| **2023-2024** | **07/07/2023** | Sincronizzazione con condition variables, Process hierarchy, `lseek` | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2023-2024/2023-07-07/exam_2023-07-07.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2023-2024/2023-07-07/solution_2023-07-07.pdf) |
| **2023-2024** | **20/06/2023** | Virtual address breakdown, Segment table + Page table, Wait channels | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2023-2024/2023-06-20/exam_2023-06-20.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2023-2024/2023-06-20/solution_2023-06-20.pdf) |
| **2022-2023** | **16/01/2023** | Gestione della memoria virtuale a 3 livelli, `fork()` COW, Syscall dispatch | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2023-01-16/exam_2023-01-16.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2023-01-16/solution_2023-01-16.pdf) |
| **2022-2023** | **26/10/2022** | TLB miss handler software, Algoritmo LRU approssimato, File descriptors | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2022-10-26/exam_2022-10-26.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2022-10-26/solution_2022-10-26.pdf) |
| **2022-2023** | **08/09/2022** | Semaphore vs Lock internals, Multilevel feedback queue, Vnode table | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2022-09-08/exam_2022-09-08.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2022-09-08/solution_2022-09-08.pdf) |
| **2022-2023** | **08/07/2022** | Paging address translation, Coremap frame allocator, `sys_dup2` | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2022-07-08/exam_2022-07-08.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2022-07-08/solution_2022-07-08.pdf) |
| **2022-2023** | **20/06/2022** | Kernel context switch step-by-step, Process states, `open` flags | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2022-06-20/exam_2022-06-20.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2022-2023/2022-06-20/solution_2022-06-20.pdf) |
| **2021-2022** | **17/01/2022** | Demand paging, TLB invalidation (`tlb_flush`), Waitpid zombies | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2022-01-17/exam_2022-01-17.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2022-01-17/solution_2022-01-17.pdf) |
| **2021-2022** | **18/10/2021** | Spinlocks with interrupts disabled, Coremap states, System call return | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2021-10-18/exam_2021-10-18.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2021-10-18/solution_2021-10-18.pdf) |
| **2021-2022** | **02/09/2021** | Two-level page tables memory overhead, Condition variables in kernel | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2021-09-02/exam_2021-09-02.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2021-09-02/solution_2021-09-02.pdf) |
| **2021-2022** | **05/07/2021** | Virtual memory fault handler, Process control block, `execv` arguments | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2021-07-05/exam_2021-07-05.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2021-07-05/solution_2021-07-05.pdf) |
| **2021-2022** | **09/06/2021** | Inverted page table hash collision resolution, OFT vs FDT structure | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2021-06-09/exam_2021-06-09.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2021-2022/2021-06-09/solution_2021-06-09.pdf) |
| **2020-2021** | **15/02/2021** | Virtual address space, Segment descriptors, Process tree management | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2020-2021/2021-02-15/exam_2021-02-15.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2020-2021/2021-02-15/solution_2021-02-15.pdf) |
| **2020-2021** | **12/09/2020** | Paging vs Segmentation, Coremap frame allocations, `fork()` PCB copy | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2020-2021/2020-09-12/exam_2020-09-12.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2020-2021/2020-09-12/solution_2020-09-12.pdf) |
| **2020-2021** | **10/07/2020** | TLB hardware structure, FIFO replacement anomaly (Belady), VFS | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2020-2021/2020-07-10/exam_2020-07-10.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2020-2021/2020-07-10/solution_2020-07-10.pdf) |
| **2020-2021** | **27/06/2020** | Syscall trap mechanism, File table reference counting, Mutexes | [📄 **Scarica Testo**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2020-2021/2020-06-27/exam_2020-06-27.pdf) | [✅ **Scarica Soluzione**](https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/2020-2021/2020-06-27/solution_2020-06-27.pdf) |

---

## 💻 Download Rapido da Terminale (CLI)

Per gli utenti che desiderano scaricare rapidamente i file direttamente dal terminale (macOS / Linux):

### 1. Scarica la Lab & Study Guide di OS/161
```bash
curl -fSL -o "OS161_LABS.pdf" \
  "https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/LABS/study_guide/OS161_LABS.pdf"
```

### 2. Scarica tutti i 48 esami e soluzioni in un unico archivio ZIP
```bash
curl -fSL -o "OS_Internals_Exams_2020_2026.zip" \
  "https://github.com/Beccaceci/PDS/raw/main/OS%20INTERNALS/EXAMS/all_exams.zip"
unzip OS_Internals_Exams_2020_2026.zip -d OS_Internals_Exams/
```

---

## 🎯 Consigli per la Preparazione dell'Esame

1. **Memoria Virtuale**: Esercitarsi sistematicamente sulla scomposizione binaria degli indirizzi virtuali (offset, indice di primo livello, indice di secondo livello). Saper calcolare esattamente l'overhead di memoria di tabelle multilivello vs tabelle invertite.
2. **TLB Miss**: Padroneggiare la sequenza di operazioni hardware/software all'insorgere di un TLB miss: ricerca nella tabella delle pagine, verifica del bit di presenza, rimpiazzo nel TLB o sollevamento di un Page Fault.
3. **Chiamate di Sistema**: Comprendere a fondo l'interazione tra la tabella dei descrittori di file di processo (FDT) e la tabella dei file aperti di sistema (OFT), in particolare l'effetto della condivisione del puntatore di lettura/scrittura post-`fork()`.
