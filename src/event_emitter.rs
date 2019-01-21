use futures::{future, prelude::*, sink, sync::mpsc};

pub struct EventEmitter<T> {
    buffer: usize,
    subscribers: Vec<mpsc::Sender<T>>,
}

impl<T> EventEmitter<T>
where
    T: Clone + Send + 'static,
{
    pub fn new(buffer: usize) -> Self {
        Self {
            buffer,
            subscribers: Vec::new(),
        }
    }

    pub fn subscribe(&mut self) -> mpsc::Receiver<T> {
        let (sender, receiver) = mpsc::channel(self.buffer);
        self.subscribers.push(sender);
        receiver
    }

    pub fn emit(&self, event: T) {
        let futures: Vec<sink::Send<mpsc::Sender<T>>> = self
            .subscribers
            .iter()
            .map(move |subscriber| subscriber.clone().send(event.clone()))
            .collect();
        let f = future::join_all(futures).map(|_| ()).map_err(|_| ());
        tokio::spawn(f);
    }
}
