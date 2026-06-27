use crossbeam::channel;

fn main() {
    let (tx, rx) = channel::bounded::<i32>(1);
    tx.shut_down_now(); // Intentional error
}
