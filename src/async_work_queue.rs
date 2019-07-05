use failure::Error;
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use log::*;
use std::sync::{Arc, Mutex, Weak};

#[derive(Clone)]
pub struct AsyncWorkQueue<T> {
    inner: Arc<Mutex<Inner>>,
    sender: mpsc::UnboundedSender<PendingWork<T>>,
}

struct Inner {
    last_id: u64,
}

struct PendingWork<T> {
    id: u64,
    data: T,
    sender: oneshot::Sender<()>,
}

impl<T> AsyncWorkQueue<T>
where
    T: Send + 'static,
{
    pub fn new<F>(f: F) -> Self
    where
        F: Send + FnMut(T) -> Result<bool, Error> + 'static,
    {
        let inner = Inner { last_id: 0 };
        let inner_p = Arc::new(Mutex::new(inner));
        let handle = Arc::downgrade(&inner_p);
        let (sender, receiver) = mpsc::unbounded();
        tokio::spawn(run_work_queue(f, handle, receiver));
        Self {
            inner: inner_p,
            sender,
        }
    }

    pub fn push(&self, id: u64, data: T) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let f = self
            .sender
            .clone()
            .send(PendingWork { id, data, sender })
            .then(|_| Ok(()));
        tokio::spawn(f);
        receiver
    }

    pub fn last_handled(&self) -> u64 {
        self.inner.lock().unwrap().last_id
    }
}

fn run_work_queue<F, T>(
    mut func: F,
    handle: Weak<Mutex<Inner>>,
    receiver: mpsc::UnboundedReceiver<PendingWork<T>>,
) -> impl Future<Item = (), Error = ()>
where
    F: FnMut(T) -> Result<bool, Error>,
{
    receiver
        .for_each(move |work| {
            let result = func(work.data).unwrap_or_else(|err| {
                error!("Work failed: {}", err);
                false
            });
            if result {
                let handle = handle.upgrade().unwrap();
                handle.lock().unwrap().last_id = work.id;
            }
            // if receiver is dropped, we don't care
            let _ = work.sender.send(());
            Ok(())
        })
        .map_err(|_| error!("Work queue failed"))
}

#[cfg(test)]
mod test {
    use super::AsyncWorkQueue;
    use futures::prelude::*;

    #[test]
    fn test_push() {
        tokio::run(futures::future::lazy(move || {
            let q = AsyncWorkQueue::new(|()| Ok(true));
            assert_eq!(q.last_handled(), 0);
            let handle = q.push(1, ());
            assert_eq!(q.last_handled(), 0);
            handle.then(move |_| {
                assert_eq!(q.last_handled(), 1);
                Ok(())
            })
        }));
    }
}
