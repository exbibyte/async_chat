/// this exercise follows the [book.async.rs/tutorial/index.html]
use {
    async_std::{
        io::BufReader,
        net::{TcpListener, TcpStream, ToSocketAddrs},
        prelude::*,
        task,
    },
    futures::{channel::mpsc, select, sink::SinkExt, FutureExt},
    std::{
        collections::hash_map::{Entry, HashMap},
        sync::Arc,
    },
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

type Sender<T> = mpsc::UnboundedSender<T>;
type Receiver<T> = mpsc::UnboundedReceiver<T>;

#[derive(Debug)]
enum Void {}

fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    //wrap the fut in an outer async and await on it for result
    task::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })
}

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?; // return box error trait obj if necessary
    let mut incoming = listener.incoming();

    let (broker_sender, broker_receiver) = mpsc::unbounded();
    let broker_handle = task::spawn(broker_loop(broker_receiver));

    while let Some(stream) = incoming.next().await {
        //async loop, ideally in for loop in the future
        let stream = stream?;
        println!("accept from: {}", stream.peer_addr()?);
        let _ = spawn_and_log_error(connection_loop(broker_sender.clone(), stream));
    }

    //finished processing all
    drop(broker_sender);

    broker_handle.await?;

    Ok(())
}

//receive connection and get content
async fn connection_loop(mut broker: Sender<Event>, stream: TcpStream) -> Result<()> {
    let stream = Arc::new(stream);
    let reader = BufReader::new(&*stream);
    let mut lines = reader.lines();

    //parse name
    let name = match lines.next().await {
        None => Err("peer disconnected")?,
        Some(line) => line?,
    };

    println!("name: {}", name);

    let (_shutdown_sender_auto_drop, shutdown_receiver) = mpsc::unbounded::<Void>();

    broker
        .send(Event::NewPeer {
            name: name.clone(),
            stream: Arc::clone(&stream),
            shutdown: shutdown_receiver,
        })
        .await
        .unwrap();

    //parse remaining destination and msg
    while let Some(line) = lines.next().await {
        let line = line?;
        let (dest, msg) = match line.find(':') {
            None => continue,
            Some(idx) => (&line[..idx], line[idx + 1..].trim()),
        };
        let dest: Vec<String> = dest
            .split(',')
            .map(|name| name.trim().to_string())
            .collect();
        let msg: String = msg.to_string();

        broker
            .send(Event::Message {
                from: name.clone(),
                to: dest,
                msg: msg,
            })
            .await
            .unwrap();
    }

    Ok(())
}

//send messages
async fn connection_writer_loop(
    messages: &mut Receiver<String>,
    stream: Arc<TcpStream>,
    shutdown: Receiver<Void>,
) -> Result<()> {
    let mut stream = &*stream;
    let mut messages = messages.fuse();
    let mut shutdown = shutdown.fuse();

    loop {
        select! {
            msg = messages.next().fuse() => match msg {//need to be fused for select!(..) in a loop
                Some(msg) => stream.write_all(msg.as_bytes()).await?,
                None => break,
            },
            void = shutdown.next().fuse() => match void {
                Some(void) => match void {},
                None => break, //shutdown
            }
        }
    }

    Ok(())
}

//using actor model for serialization
#[derive(Debug)]
enum Event {
    NewPeer {
        name: String,
        stream: Arc<TcpStream>,
        shutdown: Receiver<Void>,
    },
    Message {
        from: String,
        to: Vec<String>,
        msg: String,
    },
}

//manages mapping between peers and initiates serialization into streams
async fn broker_loop(events: Receiver<Event>) -> Result<()> {
    let mut peers: HashMap<String, Sender<String>> = HashMap::new();

    //discon_sender lives until the end
    let (discon_sender, mut discon_receiver) = mpsc::unbounded::<(String, Receiver<String>)>();

    let mut events = events.fuse();

    loop {
        let event = select! {
            event = events.next().fuse() => match event {
                None => break,
                Some(event) => event,
            },
            disconnect = discon_receiver.next().fuse() => {
                let (name, _pending_msg) = disconnect.unwrap();
                assert!(peers.remove(&name).is_some());
                continue;
            },
        };

        match event {
            Event::Message { from, to, msg } => {
                for addr in to {
                    //write to existing stream
                    if let Some(peer) = peers.get_mut(&addr) {
                        let msg = format!("from {}: {}\n", from, msg);
                        peer.send(msg).await?
                    }
                }
            }
            Event::NewPeer {
                name,
                stream,
                shutdown,
            } => match peers.entry(name.clone()) {
                Entry::Occupied(..) => {}
                Entry::Vacant(entry) => {
                    let (client_sender, mut client_receiver) = mpsc::unbounded();
                    entry.insert(client_sender);
                    let mut discon_sender_clone = discon_sender.clone();
                    //new writer for a stream
                    spawn_and_log_error(async move {
                        let res =
                            connection_writer_loop(&mut client_receiver, stream, shutdown).await;
                        discon_sender_clone
                            .send((name, client_receiver))
                            .await
                            .unwrap();
                        res
                    });
                }
            },
        }
    }

    //drop senders
    drop(peers);

    drop(discon_sender);

    while let Some((_name, _pending_msg)) = discon_receiver.next().await {}

    Ok(())
}

fn main() -> Result<()> {
    let fut = accept_loop("127.0.0.1:8080");
    //run async through an executor, block until done
    task::block_on(fut)
}
