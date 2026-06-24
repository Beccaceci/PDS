#![allow(dead_code)]
//! Esercizio 5 - Mini grep multi-file multi-thread
//!
//! Implementare la funzione `parallel_grep` che cerca un pattern letterale
//! in un elenco di file usando `n_workers` thread che si distribuiscono
//! il lavoro tramite una coda condivisa.
//!
//! Vedi il file `Lab5.md` per la specifica completa.

use std::sync::{Arc, Mutex};
use std::{fs, thread};
use std::cmp::min;
use std::io::{BufRead, BufReader};

/// Singolo riscontro: file in cui è stato trovato, numero di riga (1-based) e contenuto.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub file: String,
    pub line_number: usize,
    pub line: String,
}

impl Match {

    // create an empty instance of Match
    fn new() -> Self{
        Self {
            file: String::new(),
            line_number: 0,
            line: String::new(),
        }
    }
    fn with_line(_file: &str, _line_number: usize, _line: &str) -> Self {
        Self {
            file: _file.to_string(),
            line_number: _line_number,
            line: _line.to_string()
        }
    }
}

/// Esegue la ricerca di `pattern` (sottostringa letterale) in tutti i file
/// elencati in `paths`, usando `n_workers` thread che si distribuiscono i file
/// tramite una coda condivisa.
///
/// L'ordine dei `Match` nel vettore restituito non è specificato.
pub fn parallel_grep(_pattern: String, _paths: Vec<String>, _n_workers: usize) -> Vec<Match> {
    // Shared queue containing all file paths to process
    let paths_queue = Arc::new(Mutex::new(_paths));

    // Shared vector where all matches will be collected
    let global_results = Arc::new(Mutex::new(Vec::new()));

    let mut threads = Vec::new();

    // Spawn the specified number of worker threads
    for _ in 0.._n_workers {
        let cloned_paths_queue = paths_queue.clone();
        let cloned_global_results = global_results.clone();
        let cloned_pattern = _pattern.clone();

        threads.push(thread::spawn(move || {
            // Local vector to store matches found by this thread
            let mut thread_local_matches = Vec::new();

            // Continuously process files until the queue is empty
            loop {
                // Take one file path from the shared queue
                let file_path = {
                    let mut queue = cloned_paths_queue.lock().unwrap();
                    match queue.pop() {
                        Some(path) => path,
                        None => break, // No more files to process
                    }
                };

                // Open the file
                let file = match fs::File::open(&file_path) {
                    Ok(f) => f,
                    Err(_) => continue, // Skip file if it cannot be opened
                };

                let reader = BufReader::new(file);

                // Read the file line by line
                for (num_line, line_result) in reader.lines().enumerate() {
                    if let Ok(line_content) = line_result {
                        // Check if the current line contains the pattern
                        if line_content.contains(&cloned_pattern) {
                            // Create a Match object and store it locally
                            thread_local_matches.push(Match::with_line(
                                &file_path,
                                num_line + 1, // Convert to 1-based line numbering
                                &line_content,
                            ));
                        }
                    }
                }
            }

            // After processing all assigned files, merge local results
            // into the shared global vector
            if !thread_local_matches.is_empty() {
                let mut global = cloned_global_results.lock().unwrap();
                global.extend(thread_local_matches);
            }
        }));
    }

    // Wait for all threads to complete execution
    for thread in threads {
        thread.join().unwrap();
    }

    // Extract the final results from the shared structure
    Arc::try_unwrap(global_results)
        .expect("Arc still has multiple owners")
        .into_inner()
        .expect("Mutex was poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    fn crea_file_temp(nome: &str, contenuto: &str) -> PathBuf {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("es5_{}_{}.txt", std::process::id(), nome));
        let mut f = File::create(&path).expect("creazione file di test fallita");
        f.write_all(contenuto.as_bytes()).expect("scrittura fallita");
        path
    }

    #[test]
    fn grep_singolo_file() {
        let p = crea_file_temp(
            "single",
            "prima riga\nseconda riga con TARGET\nterza riga\n",
        );
        let paths = vec![p.to_string_lossy().to_string()];
        let res = parallel_grep("TARGET".to_string(), paths, 2);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].line_number, 2);
        assert!(res[0].line.contains("TARGET"));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn grep_nessun_match() {
        let p = crea_file_temp("nomatch", "una\ndue\ntre\n");
        let paths = vec![p.to_string_lossy().to_string()];
        let res = parallel_grep("zzz".to_string(), paths, 2);
        assert!(res.is_empty());
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn grep_piu_match_per_file() {
        let p = crea_file_temp(
            "many",
            "foo bar\nfoo baz\nbar\nfoofoo\n",
        );
        let paths = vec![p.to_string_lossy().to_string()];
        let res = parallel_grep("foo".to_string(), paths, 1);
        assert_eq!(res.len(), 3);
        let righe: Vec<usize> = res.iter().map(|m| m.line_number).collect();
        assert!(righe.contains(&1));
        assert!(righe.contains(&2));
        assert!(righe.contains(&4));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn grep_piu_file() {
        let p1 = crea_file_temp("f1", "alfa\nbeta\nciao mondo\n");
        let p2 = crea_file_temp("f2", "ciao\nrust\nciao ciao\n");
        let paths = vec![
            p1.to_string_lossy().to_string(),
            p2.to_string_lossy().to_string(),
        ];
        let res = parallel_grep("ciao".to_string(), paths, 4);
        assert_eq!(res.len(), 3);
        let _ = std::fs::remove_file(&p1);
        let _ = std::fs::remove_file(&p2);
    }

    #[test]
    fn grep_ignora_file_inesistenti() {
        let p = crea_file_temp("ok", "questa contiene XYZ\naltra riga\n");
        let paths = vec![
            "/percorso/che/non/esiste.txt".to_string(),
            p.to_string_lossy().to_string(),
        ];
        let res = parallel_grep("XYZ".to_string(), paths, 3);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].line_number, 1);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn grep_match_associato_al_file_corretto() {
        let p1 = crea_file_temp("a", "TARGET qui\n");
        let p2 = crea_file_temp("b", "niente da vedere\n");
        let s1 = p1.to_string_lossy().to_string();
        let s2 = p2.to_string_lossy().to_string();
        let res = parallel_grep("TARGET".to_string(), vec![s1.clone(), s2.clone()], 2);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].file, s1);
        let _ = std::fs::remove_file(&p1);
        let _ = std::fs::remove_file(&p2);
    }

    #[test]
    fn grep_lista_path_vuota() {
        // Nessun file da elaborare: tutti i worker terminano senza fare nulla.
        let res = parallel_grep("qualsiasi".to_string(), vec![], 4);
        assert!(res.is_empty());
    }

    #[test]
    fn grep_tutti_file_inesistenti() {
        // Tutti i file non esistono: nessun panic, risultato vuoto.
        let paths = vec![
            "/percorso/inesistente/a.txt".to_string(),
            "/percorso/inesistente/b.txt".to_string(),
        ];
        let res = parallel_grep("foo".to_string(), paths, 3);
        assert!(res.is_empty());
    }

    #[test]
    fn grep_piu_worker_che_file() {
        // 8 worker, 1 solo file: i worker in eccesso devono terminare in
        // modo pulito quando trovano la coda vuota.
        let p = crea_file_temp("solo", "TARGET\nnope\nTARGET\n");
        let paths = vec![p.to_string_lossy().to_string()];
        let res = parallel_grep("TARGET".to_string(), paths, 8);
        assert_eq!(res.len(), 2);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn grep_pattern_ripetuto_nella_stessa_riga_conta_una_volta() {
        // Una riga che contiene il pattern più volte deve produrre
        // un solo Match.
        let p = crea_file_temp("ripetuto", "foo foo foo\nbar\nfoofoofoo\n");
        let paths = vec![p.to_string_lossy().to_string()];
        let res = parallel_grep("foo".to_string(), paths, 2);
        assert_eq!(res.len(), 2);
        let righe: Vec<usize> = res.iter().map(|m| m.line_number).collect();
        assert!(righe.contains(&1));
        assert!(righe.contains(&3));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn grep_pattern_case_sensitive() {
        // Il lab specifica una ricerca per sottostringa letterale: deve
        // distinguere maiuscole/minuscole.
        let p = crea_file_temp(
            "case",
            "Foo qui\nfoo qui\nFOO qui\n",
        );
        let paths = vec![p.to_string_lossy().to_string()];
        let res = parallel_grep("foo".to_string(), paths, 2);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].line_number, 2);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn grep_distribuzione_carico_su_molti_file() {
        // Crea molti file e verifica che il numero totale di match sia
        // corretto indipendentemente dall'ordine di elaborazione.
        let mut files = Vec::new();
        let mut paths = Vec::new();
        for i in 0..20 {
            // Ciascun file contiene esattamente 2 match del pattern.
            let p = crea_file_temp(
                &format!("dist_{}", i),
                "noise\nNEEDLE qui\nnoise\nbella NEEDLE!\nfine\n",
            );
            paths.push(p.to_string_lossy().to_string());
            files.push(p);
        }
        let res = parallel_grep("NEEDLE".to_string(), paths, 4);
        assert_eq!(res.len(), 40);
        // Ogni file deve apparire esattamente 2 volte nei risultati.
        for f in &files {
            let s = f.to_string_lossy().to_string();
            let n = res.iter().filter(|m| m.file == s).count();
            assert_eq!(n, 2, "file {} ha prodotto {} match invece di 2", s, n);
        }
        for f in files {
            let _ = std::fs::remove_file(&f);
        }
    }

    #[test]
    fn grep_numero_riga_e_contenuto_corretti() {
        let p = crea_file_temp(
            "righe",
            "alfa\nbeta NEEDLE beta\ngamma\ndelta NEEDLE\n",
        );
        let paths = vec![p.to_string_lossy().to_string()];
        let res = parallel_grep("NEEDLE".to_string(), paths, 1);
        assert_eq!(res.len(), 2);

        let mut by_line: Vec<&Match> = res.iter().collect();
        by_line.sort_by_key(|m| m.line_number);
        assert_eq!(by_line[0].line_number, 2);
        assert_eq!(by_line[0].line, "beta NEEDLE beta");
        assert_eq!(by_line[1].line_number, 4);
        assert_eq!(by_line[1].line, "delta NEEDLE");
        let _ = std::fs::remove_file(&p);
    }
}
