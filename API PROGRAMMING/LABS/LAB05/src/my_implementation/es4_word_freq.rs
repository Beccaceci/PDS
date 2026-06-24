#![allow(dead_code)]
use std::collections::HashMap;
use std::{fs, thread};
use std::sync::{Arc, Mutex};

/// Computes the global frequency of all words across multiple files.
///
/// Each file is processed independently in a separate thread.
/// Each thread builds a local frequency map, which is later merged
/// into a shared global map.
pub fn word_frequencies(_paths: Vec<String>) -> HashMap<String, usize> {
    // Shared global map where all threads will accumulate results
    let global_hashmap: Arc<Mutex<HashMap<String, usize>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Vector to store thread handles
    let mut threads = Vec::new();

    // Iterate over all file paths
    for i in 0.._paths.len() {
        // Clone the shared map reference for the thread
        let cloned_global_hashmap = global_hashmap.clone();

        // Clone the path so it can be moved into the thread
        let path = _paths[i].clone();

        // Spawn a new thread to process this file
        threads.push(thread::spawn(move || {
            // Read file content into a string
            let content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(_) => return, // If file cannot be read, skip it
            };

            // Local map to count word frequencies for this file
            let mut local_map = HashMap::new();

            // Split content into words:
            // - non-alphanumeric characters are used as separators
            // - empty strings are ignored
            // - words are converted to lowercase
            for word in content
                .split(|c: char| !c.is_alphanumeric())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_lowercase())
            {
                // Increment count for each word
                *local_map.entry(word).or_insert(0) += 1;
            }

            // Merge the local map into the global map
            let mut global = cloned_global_hashmap.lock().unwrap();
            for (key, value) in local_map {
                *global.entry(key).or_insert(0) += value;
            }
        }));
    }

    // Wait for all threads to finish execution
    for thread in threads {
        thread.join().unwrap();
    }

    // Extract the final HashMap from Arc<Mutex<...>>
    Arc::try_unwrap(global_hashmap)
        .expect("Arc still has multiple owners")
        .into_inner()
        .expect("Mutex was poisoned")
}

/// Returns the top k most frequent words.
/// Words are sorted by:
/// 1. Frequency (descending)
/// 2. Alphabetical order (ascending) in case of ties
pub fn top_k(_freqs: &HashMap<String, usize>, _k: usize) -> Vec<(String, usize)> {
    let mut top_k: Vec<(String, usize)> = Vec::new();

    // If k is zero or the map is empty, return an empty vector
    if _k == 0 || _freqs.is_empty() {
        return top_k;
    }

    // Convert the HashMap into a vector of (word, count) pairs
    top_k = _freqs
        .iter()
        .map(|(word, &count)| (word.clone(), count))
        .collect();

    // Sort the vector:
    // - higher frequency first
    // - if equal frequency, sort alphabetically
    top_k.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.0.cmp(&b.0))
    });

    // If k is greater than or equal to the number of elements,
    // return the entire sorted vector
    if _k >= _freqs.len() {
        return top_k;
    }

    // Otherwise, return only the first k elements
    top_k.into_iter().take(_k).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    /// Crea un file temporaneo con il contenuto indicato e ne restituisce il percorso.
    fn crea_file_temp(nome: &str, contenuto: &str) -> PathBuf {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("es4_{}_{}.txt", std::process::id(), nome));
        let mut f = File::create(&path).expect("creazione file di test fallita");
        f.write_all(contenuto.as_bytes())
            .expect("scrittura fallita");
        path
    }

    #[test]
    fn frequenze_singolo_file() {
        let p = crea_file_temp("singolo", "Ciao ciao mondo");
        let paths = vec![p.to_string_lossy().to_string()];
        let f = word_frequencies(paths);
        assert_eq!(f.get("ciao"), Some(&2));
        assert_eq!(f.get("mondo"), Some(&1));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn frequenze_piu_file() {
        let p1 = crea_file_temp("a", "rust è bello rust");
        let p2 = crea_file_temp("b", "Rust va veloce");
        let paths = vec![
            p1.to_string_lossy().to_string(),
            p2.to_string_lossy().to_string(),
        ];
        let f = word_frequencies(paths);
        assert_eq!(f.get("rust"), Some(&3));
        assert_eq!(f.get("bello"), Some(&1));
        assert_eq!(f.get("veloce"), Some(&1));
        let _ = std::fs::remove_file(&p1);
        let _ = std::fs::remove_file(&p2);
    }

    #[test]
    fn file_inesistente_non_interrompe() {
        let p = crea_file_temp("ok", "uno due tre");
        let paths = vec![
            "/percorso/che/non/esiste/davvero.txt".to_string(),
            p.to_string_lossy().to_string(),
        ];
        let f = word_frequencies(paths);
        assert_eq!(f.get("uno"), Some(&1));
        assert_eq!(f.get("due"), Some(&1));
        assert_eq!(f.get("tre"), Some(&1));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn punteggiatura_e_case() {
        let p = crea_file_temp("punct", "Hello, world! HELLO world.");
        let paths = vec![p.to_string_lossy().to_string()];
        let f = word_frequencies(paths);
        assert_eq!(f.get("hello"), Some(&2));
        assert_eq!(f.get("world"), Some(&2));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn top_k_ordinamento() {
        let mut m = HashMap::new();
        m.insert("alfa".to_string(), 5);
        m.insert("beta".to_string(), 5);
        m.insert("gamma".to_string(), 10);
        m.insert("delta".to_string(), 1);

        let top = top_k(&m, 3);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0], ("gamma".to_string(), 10));
        // Parità a 5: ordine alfabetico crescente => alfa prima di beta.
        assert_eq!(top[1], ("alfa".to_string(), 5));
        assert_eq!(top[2], ("beta".to_string(), 5));
    }

    #[test]
    fn top_k_piu_grande_della_mappa() {
        let mut m = HashMap::new();
        m.insert("uno".to_string(), 1);
        m.insert("due".to_string(), 2);
        let top = top_k(&m, 10);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "due");
    }

    #[test]
    fn lista_di_path_vuota() {
        let f = word_frequencies(vec![]);
        assert!(f.is_empty());
    }

    #[test]
    fn file_vuoto() {
        let p = crea_file_temp("vuoto", "");
        let paths = vec![p.to_string_lossy().to_string()];
        let f = word_frequencies(paths);
        assert!(f.is_empty());
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn tutti_i_file_inesistenti() {
        // Nessun file leggibile: il programma non deve panicare e deve
        // restituire una mappa vuota.
        let paths = vec![
            "/percorso/inesistente/a.txt".to_string(),
            "/percorso/inesistente/b.txt".to_string(),
        ];
        let f = word_frequencies(paths);
        assert!(f.is_empty());
    }

    #[test]
    fn frequenze_su_piu_righe() {
        let p = crea_file_temp(
            "multi_riga",
            "alfa beta\nbeta gamma\nalfa\n",
        );
        let paths = vec![p.to_string_lossy().to_string()];
        let f = word_frequencies(paths);
        assert_eq!(f.get("alfa"), Some(&2));
        assert_eq!(f.get("beta"), Some(&2));
        assert_eq!(f.get("gamma"), Some(&1));
        assert_eq!(f.len(), 3);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn parole_alfanumeriche() {
        // I caratteri non alfanumerici fanno da separatore: "abc123" è
        // un'unica parola, "abc-123" diventa due.
        let p = crea_file_temp("alnum", "abc123 abc-123\nabc123");
        let paths = vec![p.to_string_lossy().to_string()];
        let f = word_frequencies(paths);
        assert_eq!(f.get("abc123"), Some(&2));
        assert_eq!(f.get("abc"), Some(&1));
        assert_eq!(f.get("123"), Some(&1));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn top_k_zero() {
        let mut m = HashMap::new();
        m.insert("a".to_string(), 1);
        m.insert("b".to_string(), 2);
        assert!(top_k(&m, 0).is_empty());
    }

    #[test]
    fn top_k_su_mappa_vuota() {
        let m: HashMap<String, usize> = HashMap::new();
        assert!(top_k(&m, 5).is_empty());
    }

    #[test]
    fn top_k_ordine_decrescente_completo() {
        let mut m = HashMap::new();
        m.insert("a".to_string(), 3);
        m.insert("b".to_string(), 7);
        m.insert("c".to_string(), 1);
        m.insert("d".to_string(), 5);
        let top = top_k(&m, 4);
        let counts: Vec<usize> = top.iter().map(|(_, c)| *c).collect();
        // Devono essere ordinate strettamente in modo decrescente.
        for w in counts.windows(2) {
            assert!(w[0] >= w[1]);
        }
        assert_eq!(top[0], ("b".to_string(), 7));
        assert_eq!(top[3], ("c".to_string(), 1));
    }

    #[test]
    fn fusione_mappa_globale_e_parallelismo() {
        // Stress: molti file di piccola dimensione, ognuno elaborato in
        // parallelo. La somma globale per parola deve essere coerente con
        // la somma sequenziale.
        let mut paths = Vec::new();
        for i in 0..16 {
            let contenuto = format!("alfa beta gamma\nalfa alfa\n{} delta", i);
            paths.push(crea_file_temp(&format!("parallel_{}", i), &contenuto));
        }
        let path_strings: Vec<String> = paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        let f = word_frequencies(path_strings);
        // Ogni file contribuisce con: alfa x3, beta x1, gamma x1, delta x1.
        assert_eq!(f.get("alfa"), Some(&(3 * 16)));
        assert_eq!(f.get("beta"), Some(&16));
        assert_eq!(f.get("gamma"), Some(&16));
        assert_eq!(f.get("delta"), Some(&16));
        for p in paths {
            let _ = std::fs::remove_file(&p);
        }
    }
}
