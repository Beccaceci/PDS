//! # Simulazione 047 — RpcClient
//!
//! Tabella di correlazione richiesta/risposta al cuore di un client RPC con demultiplazione
//! concorrente e fuori ordine[cite: 11]. Gestisce la chiusura pulita (`close`)[cite: 11], la notifica asincrona
//! (`deliver_response`)[cite: 11], e la concorrenza tra l'attesa (`wait_response`)[cite: 11] e l'abbandono RAII (`Drop`)[cite: 11].
//!
//! ### Requisiti
//! - Thread-safe, condivisibile (`Clone + Send + Sync`)[cite: 11].
//! - `send()` non mantiene alcun lock durante l'esecuzione della closure `transport`[cite: 11].
//! - `deliver_response` non blocca mai[cite: 11].
//! - `PendingRequest` pulisce il proprio id tramite `Drop` se esce dallo scope senza chiamata a `wait_response`[cite: 11].
//! - `close()` sblocca tutte le richieste pendenti (e future) con `None`[cite: 11].
//! - Nessuna attesa attiva (`Mutex` + `Condvar`)[cite: 11].

use std::{marker::PhantomData, mem::replace, sync::{Arc, Condvar, Mutex}};

use crate::ResponseState::{Closed, NotYetArrived, Delivered};

/// Handle restituito da `send()`, che permette l'attesa della risposta correlata[cite: 11].
pub trait PendingRequest<Resp: Send> {
    /// Consuma questo handle, bloccando il chiamante finché la risposta correlata non arriva
    /// o finché il client non viene chiuso (in tal caso restituisce `None`)[cite: 11].
    fn wait_response(self) -> Option<Resp>;
}

/// Tratto che rappresenta il client RPC generico per richieste `Req` e risposte `Resp`[cite: 11].
pub trait RpcClient<Req: Send, Resp: Send>: Clone + Send + Sync {
    /// Invia una richiesta generando un identificatore univoco ed eseguendo `transport` fuori dal lock[cite: 11].
    fn send(
        &self,
        request: Req,
        transport: impl FnOnce(u64, Req) + Send + 'static,
    ) -> impl PendingRequest<Resp>;

    /// Consegna la risposta per l'identificatore dato senza mai bloccare[cite: 11].
    fn deliver_response(&self, id: u64, response: Resp);

    /// Chiude il client sbloccando tutte le richieste pendenti con `None`[cite: 11].
    fn close(&self);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyPendingRequest<Resp: Send> {
    response: Arc<(Mutex<ResponseState<Resp>>, Condvar)>
}

impl<Resp: Send> PendingRequest<Resp> for MyPendingRequest<Resp> {
    fn wait_response(self) -> Option<Resp> {
        let (mutex, cvar) = &*self.response;
        let mut guard = mutex.lock().unwrap();
        guard = cvar.wait_while(guard, |c| {
            matches!(*c, NotYetArrived)
        }).unwrap();

        if let Delivered(response) = replace(&mut *guard, NotYetArrived) {
            Some(response)
        }
        else {
            None
        }
    }
}

impl<Resp: Send> Drop for MyPendingRequest<Resp> {
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.response;
        let mut guard = mutex.lock().unwrap();
        *guard = Closed;
        drop(guard);
        cvar.notify_one();
    }
}

pub enum ResponseState<Resp: Send> {
    NotYetArrived,
    Closed,
    Delivered(Resp)
}

pub struct RpcClientState<Resp: Send> {
    responses: Vec<Arc<(Mutex<ResponseState<Resp>>, Condvar)>>,
    next_id: u64,
    closed: bool
}

impl<Resp: Send> RpcClientState<Resp> {
    pub fn new () -> Self {
        Self {
            responses: Vec::new(),
            next_id: 1,
            closed: false
        }
    }
}

pub struct MyRpcClient<Req: Send, Resp: Send> {
    inner: Arc<Mutex<RpcClientState<Resp>>>,
    req: PhantomData<fn() -> Req>
}

impl<Req: Send, Resp: Send> MyRpcClient<Req, Resp> {
    pub fn new () -> Self {
        Self {
            inner: Arc::new(Mutex::new(RpcClientState::new())),
            req: PhantomData
        }
    }
}

impl<Req: Send, Resp: Send> RpcClient<Req, Resp> for MyRpcClient<Req, Resp> {
    fn send(
        &self,
        request: Req,
        transport: impl FnOnce(u64, Req) + Send + 'static,
    ) -> impl PendingRequest<Resp>
    {
        let mut client_guard = self.inner.lock().unwrap();
        let actual_id = client_guard.next_id;
        client_guard.next_id += 1;

        let new_state = if client_guard.closed { Closed } else { NotYetArrived };
        let new_response = Arc::new((Mutex::new(new_state), Condvar::new()));

        client_guard.responses.push(new_response.clone());
        drop(client_guard);
        
        transport(actual_id, request);

        MyPendingRequest {
            response: new_response
        }
    }

    fn deliver_response(&self, id: u64, response: Resp) {
        let client_guard = &mut *self.inner.lock().unwrap();
        if !client_guard.closed {
            if let Some(target_response) = client_guard.responses.get((id-1) as usize) {
                let (mutex_response, cvar_response) = &**target_response;
                let mut guard_response = mutex_response.lock().unwrap();
                *guard_response = Delivered(response);
                drop(guard_response);
                cvar_response.notify_all();
            }
        }
    }

    fn close(&self) {
        let client_guard = &mut *self.inner.lock().unwrap();
        client_guard.closed = true;

        for response in client_guard.responses.iter() {
            let (mutex_response, cvar_response) = &**response;
            let mut guard_response = mutex_response.lock().unwrap();
            *guard_response = Closed;
            drop(guard_response);
            cvar_response.notify_all();  
        }
    }
}

impl<Req: Send, Resp: Send> Clone for MyRpcClient<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            req: PhantomData
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un nuovo `RpcClient`[cite: 11].
pub fn make_rpc_client<Req: Send + 'static, Resp: Send + 'static>() -> impl RpcClient<Req, Resp> {
    MyRpcClient::<Req, Resp>::new()
}