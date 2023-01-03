use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Receiver;
use std::{fs, io, thread};
use std::io::{Read, Write};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

static mut RUNTIME: *mut Runtime = std::ptr::null_mut();


pub struct Runtime {
    ///线程池可用的线程
    available_threads: Vec<usize>,
    ///预订执行的回调函数
    callbacks_to_run: Vec<(usize, Js)>,
    ///注册过的所有回调函数
    callback_queue: HashMap<usize, Box<dyn FnOnce(Js)>>,
    ///等待中的epoll事件的数量，在这个例子中只是是我们用来打印信息的
    epoll_pending_events: usize,
    ///事件注册器，用于向操作系统注册对时间的兴趣
    epoll_registrator: minimio::Registrator,
    ///epoll 线程的句柄
    epoll_thread: thread::JoinHandle<()>,
    ///None 无限长, Some(n) = 在N毫秒后超市 some(0)立即
    epoll_timeout: Arc<Mutex<Option<i32>>>,
    ///管道 线程池和epoll线程通过管道向主时间循环发送事件
    event_reciever: Receiver<PollEvent>,
    ///回调创建唯一标识
    identity_token: usize,
    ///待执行的时间数量为0时，程序结束
    pending_events: usize,
    ///线程池中的线程对应的句柄
    thread_pool: Vec<NodeThread>,
    ///保存所有的定时器和对应的回调的id
    timers: BTreeMap<Instant, usize>,
    ///临时保存需要移除的定时器。让运行时持有它们，这样我们就可以复用这些定时器
    timers_to_remove: Vec<Instant>,
}

impl Runtime {
    pub fn run(mut self, f: impl Fn()) {
        let rt_ptr: *mut Runtime = &mut self;
        unsafe { RUNTIME = rt_ptr };

        // 在执行过程中用来打印循环的轮次
        let mut ticks = 0;

        // 首先执行“main”函数（也就是主线程中的函数）
        f();

        // ===== EVENT LOOP =====
        // ===== 事件循环 =====
        while self.pending_events > 0 {
            ticks += 1;
            // 不是循环的一部分，只是为了让我们更清晰地看到正在执行的时刻
            print!("===== TICK {} =====", ticks);

            // ===== 1. TIMERS =====
            // ===== 1. 定时器 =====
            self.process_expired_timers();

            // ===== 2. CALLBACKS =====
            // ===== 2. 回调函数 =====
            // 定时器的回调函数，有可能出于某些原因，我们会把这些回调函数放在下一轮循环中执行。
            // 虽然在我们的实现中不可能。
            self.run_callbacks();

            // ===== 3. IDLE/PREPARE =====
            // ===== 3. 空闲和准备 =====
            // 我们不会用到这个部分

            // ===== 4. POLL =====
            // ===== 4. 轮询 =====
            // 首先检查是否有尚未解决的事件，如果没有，那么事件循环就结束了
            // 如果有的话，就一直等待
            if self.pending_events == 0 {
                break;
            }

            // 计算到下一个定时器的超时的时间差，作为epoll wait的超时时间。
            // 如果没有定时器，那么设置为无限等待。
            let next_timeout = self.get_next_timer();

            let mut epoll_timeout_lock = self.epoll_timeout.lock().unwrap();
            *epoll_timeout_lock = next_timeout;
            // 在`recv`函数等待前，释放锁。
            drop(epoll_timeout_lock);

            // 虽然我们是一件一件地去处理事件的，但是在一次poll中可能会有多个事件返回。
            // 我们不会在这里讨论这个问题，但是确实有几种方式来处理这个问题。
            if let Ok(event) = self.event_reciever.recv() {
                match event {
                    PollEvent::Timeout => (),
                    PollEvent::ThreadPool((thread_id, callback_id, data)) => {
                        self.process_thread_pool_events(thread_id, callback_id, data);
                    }
                    PollEvent::Epoll(event_id) => {
                        self.process_epoll_events(event_id);
                    }
                }
            }
            self.run_callbacks();

            // ===== 5. CHECK =====
            // 高负载下服务端可能会有大量的回调函数需要合理地在一次poll中执行，这也意味着在等待这些回调执行结束的同时，I/O资源太过空闲。生产环境下的运行时还需要考虑其他类似的情况。
            // 可以把 setImmediate函数添加到此处，但在这里我们并不会这么做。

            // ===== 6. CLOSE CALLBACKS ======
            // 释放资源，但是我们不在此处释放资源。
            // 但通常来说，释放资源在此处完成，比如关闭socket
        }

        // 释放所有资源，确保所有的析构函数都执行。
        for thread in self.thread_pool.into_iter() {
            thread.sender.send(Task::close()).expect("threadpool cleanup");
            thread.handle.join().unwrap();
        }

        self.epoll_registrator.close_loop().unwrap();
        self.epoll_thread.join().unwrap();

        println!("FINISHED");
    }

    //process_expired_timers 检查是否有到期的定时器
    fn process_expired_timers(&mut self) {
        //为了满足 borrow checker 所以使用一个中间变量
        let timers_to_remove = &mut self.timers_to_remove;
        self.timers.range(..=Instant::now())
            .for_each(|(k, _)| timers_to_remove.push(*k));
        while let Some(key) = self.timers_to_remove.pop() {
            let callback_id = self.timers.remove(&key).unwrap();
            self.callbacks_to_run.push((callback_id, Js::Undefined))
        }
    }

    fn get_next_timer(&self) -> Option<i32> {
        self.timers.iter().nth(0).map(|(&instant, _)| {
            let mut time_to_next_timeout = instant - Instant::now();
            if time_to_next_timeout < Duration::new(0, 0) {
                time_to_next_timeout = Duration::new(0, 0);
            }
            time_to_next_timeout.as_millis() as i32
        })
    }

    //run_callbacks 回调处理
    fn run_callbacks(&mut self){
        while let Some((callback_id,data)) = self.callbacks_to_run.pop(){
            let cb = self.callback_queue.remove(&callback_id).unwrap();
            cb(data);
            // 这一步非常重要，正如你可能理解的那样，回调中任何长时间运行的代码都会阻塞住eventloop，
            // 使循环不能继续进行下去。这样既不能处理其他的回调，也不能注册新的事件。这就是阻塞事件循环的代码被排斥的原因。
            self.pending_events -=1;
        }
    }

    fn process_thread_pool_events(&mut self,thread_id:usize,callback_id:usize,data:Js){
        self.callbacks_to_run.push((callback_id,data));
        self.available_threads.push(thread_id);
    }

    fn process_epoll_events(&mut self,event_id:usize){
        //主要的原因就是I/O队列本身不会返回任何的数据，它只是告诉我们数据已经准备就绪，等待读取而已。
        self.callbacks_to_run.push((event_id,Js::Undefined));
        self.epoll_pending_events-=1;

    }

    //get_available_thread 用于获取一个空闲线程对应的id
    fn get_available_thread(&mut self) -> usize {
        match self.available_threads.pop() {
            Some(thread_id) => thread_id,
            None => panic!("Out of threads."),//todo：panic
        }
    }

    /// 如果达到最大值，就回到0
    fn generate_identity(&mut self) -> usize {
        self.identity_token = self.identity_token.wrapping_add(1);
        self.identity_token
    }

    fn generate_cb_identity(&mut self) -> usize {
        let ident = self.generate_identity();
        let taken = self.callback_queue.contains_key(&ident);

        // 如果出现冲突或者标识符已经存在的情况，那么我们就会一直循环，直至获取到一个不冲突、不重复的标识符。
        // 我们也不会覆盖如下场景———数量为`usize::Max`的回调等待执行，因为即使速度够快，每纳秒都能在队列中加入一个回调函数，
        // 在64位的系统上要生成这么多数量的回调依旧需要585.5年。
        if !taken {
            ident
        } else {
            loop {
                let possible_ident = self.generate_identity();
                if self.callback_queue.contains_key(&possible_ident) {
                    break possible_ident;
                }
            }
        }
    }

    /// Adds a callback to the queue and returns the key
    /// 将回调添加到队列中，并且返回一个key
    fn add_callback(&mut self, ident: usize, cb: impl FnOnce(Js) + 'static) {
        let boxed_cb = Box::new(cb);
        self.callback_queue.insert(ident, boxed_cb);
    }

    pub fn register_event_epoll(&mut self, token: usize, cb: impl FnOnce(Js) + 'static) {
        self.add_callback(token, cb);

        println!("Event with id: {} registered.", token);
        self.pending_events += 1;
        self.epoll_pending_events += 1;
    }

    ///投递 任务
    pub fn register_event_threadpool(
        &mut self,
        task: impl Fn() -> Js + Send + 'static,
        kind: ThreadPoolTaskKind,
        cb: impl FnOnce(Js) + 'static,
    ) {
        let callback_id = self.generate_cb_identity();
        self.add_callback(callback_id, cb);
        let event = Task {
            task: Box::new(task),
            callback_id,
            kind,
        };
        // we are not going to implement a real scheduler here, just a LIFO queue
        // 我们并不会在此实现一个真正的调度器，而是简化为一个LIFO队列
        let available = self.get_available_thread();
        self.thread_pool[available].sender.send(event).expect("register work");
        self.pending_events += 1;
    }


    fn set_timeout(&mut self, ms: u64, cb: impl Fn(Js) + 'static) {
        // 理论上来说，有没有可能出现获取到两个相同时刻？如果是这样的话，那这就会成为一个bug...
        let now = Instant::now();
        let cb_id = self.generate_cb_identity();
        self.add_callback(cb_id, cb);
        let timeout = now + Duration::from_millis(ms);
        self.timers.insert(timeout, cb_id);
        self.pending_events += 1;
        println!("Registered timer event id: {}", cb_id);
    }
}

impl Runtime {
    pub fn new() -> Self {
        // ===== THE REGULAR THREAD POOL =====
        let (event_sender, event_reciever) = std::sync::mpsc::channel::<PollEvent>();
        let mut threads = Vec::with_capacity(4);

        for i in 0..4 {
            let (evt_sender, evt_reciever) = std::sync::mpsc::channel::<Task>();
            let event_sender = event_sender.clone();

            let handle = thread::Builder::new()
                .name(format!("pool{}", i))
                .spawn(move || {
                    while let Ok(task) = evt_reciever.recv() {
                        println!("received a task of type: {:?}", task.kind);

                        if let ThreadPoolTaskKind::Close = task.kind {
                            break;
                        };

                        let res = (task.task)();
                        println!("finished running a task of type: {:?}.", task.kind);

                        let event = PollEvent::ThreadPool((i, task.callback_id, res));
                        event_sender.send(event).expect("threadpool");

                    }
                })
                .expect("Couldn't initialize thread pool.");

            let node_thread = NodeThread {
                handle,
                sender: evt_sender,
            };

            threads.push(node_thread);
        }

        // ===== EPOLL THREAD =====
        //首先，我们初始化一个全新的minimio::Poll对象。它是kqueue/epoll/IOCP事件队列的主入口。
        let mut poll = minimio::Poll::new().expect("Error creating epoll queue");
        let registrator = poll.registrator();
        let epoll_timeout = Arc::new(Mutex::new(None));
        let epoll_timeout_clone = epoll_timeout.clone();

        let epoll_thread = thread::Builder::new()
            .name("epoll".to_string())
            .spawn(move || {
                //首先，我们分配一个缓冲区来保存从poll实例中获取的事件对象。这些对象包含了发生事件的信息，
                // 包括我们在注册事件时传入的token。这个token标识发生了哪个事件。在我们的示例中，token是一个简单的usize。
                let mut events = minimio::Events::with_capacity(1024);
                //epoll线程将运行一个循环，该循环有意识地轮询是否新事件发生。
                loop {
                    //循环内部有一个比较有趣的逻辑——首先我们读取超时时间的值，而它应该与主循环中的下一个（最先发生的）定时器的超时时间点一致。
                    let epoll_timeout_handle = epoll_timeout_clone.lock().unwrap();
                    let timeout = *epoll_timeout_handle;
                    drop(epoll_timeout_handle);

                    //当我们提到block时，我们想表达的是：操作系统会挂起我们的线程，然后切换至其他线程的上下文。然而，
                    // 操作系统依然会跟踪 epoll线程，监听事件，并且当我们注册的感兴趣的事件发生时，它会再次唤醒epoll线程。
                    // 大致有4种情形：
                    //
                    // 返回值为Ok(n) 且n大于0，这种情况就表示我们有事件需要去处理
                    // 返回值为Ok(n)且n等于0，那么我们就能知道，这要么是一次虚假唤醒，要么是poll超时了
                    // 返回值为Err且类型为Interrupted，这种情况下，我们把这作为一种关闭信号，停止循环
                    // 返回值为Err但不是Interrupted类型，我们就知道大事不妙了，并且触发panic!
                    // 如果你之前没有见过Ok(v) if v > 0这样的语法，那么我现在告诉你：我们把这叫做match guard，这种语法使我们能够精细化匹配。在这里，我们只匹配v值大于0的情况。
                    //
                    // 为了完整起见，我再解释一下Err(ref e) if e.kind()：ref关键字是告知编译器我们只需要e的引用，并不需要取得其所有权。
                    //
                    // 最后的情况_ => unreachable!()是因为编译器并不知道我们已经覆盖了所有Ok()的可能情况。这个值的类型是Ok(usize)，所以它不可能为负数，所以这个分支的作用就是告诉编译器：我们已经覆盖了所有可能情况。
                    match poll.poll(&mut events, timeout) {
                        Ok(v) if v > 0 => {
                            for i in 0..v {
                                let event = events.get_mut(i).expect("No events in event list.");
                                println!("epoll event {} is ready", event.id());

                                let event = PollEvent::Epoll(event.id());
                                event_sender.send(event).expect("epoll event");
                            }
                        }
                        Ok(v) if v == 0 => {
                            println!("epoll event timeout is ready");
                            event_sender.send(PollEvent::Timeout).expect("epoll timeout");
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                            println!("received event of type: Close");
                            break;
                        }
                        Err(e) => panic!("{:?}", e),
                        _ => unreachable!(),
                    }
                }
            })
            .expect("Error creating epoll thread");

        Runtime {
            available_threads: (0..4).collect(),
            callbacks_to_run: vec![],
            callback_queue: HashMap::new(),
            epoll_pending_events: 0,
            epoll_registrator: registrator,
            epoll_thread,
            epoll_timeout,
            event_reciever,
            identity_token: 0,
            pending_events: 0,
            thread_pool: threads,
            timers: BTreeMap::new(),
            timers_to_remove: vec![],
        }
    }
}

struct Task {
    task: Box<dyn Fn() -> Js + Send + 'static>,
    callback_id: usize,
    kind: ThreadPoolTaskKind,
}

impl Task {
    fn close() -> Self {
        Task {
            task: Box::new(|| Js::Undefined),
            callback_id: 0,
            kind: ThreadPoolTaskKind::Close,
        }
    }
}

#[derive(Debug)]
struct NodeThread {
    pub(crate) handle: JoinHandle<()>,
    sender: std::sync::mpsc::Sender<Task>,
}

#[derive(Debug)]
pub enum ThreadPoolTaskKind {
    FileRead,
    Encrypt,
    Close,
}

#[derive(Debug)]
pub enum Js {
    Undefined,
    String(String),
    Int(usize),
}

impl Js {
    /// 简便方法，因为已知具体类型
    fn into_string(self) -> Option<String> {
        match self {
            Js::String(s) => Some(s),
            _ => None,
        }
    }

    /// 简便方法，因为已知具体类型
    fn into_int(self) -> Option<usize> {
        match self {
            Js::Int(n) => Some(n),
            _ => None,
        }
    }
}

/// 描述了Epoll事件循环能够处理的三种事件
enum PollEvent {
    /// 来自`线程池`的事件，持有一个元组，包含`thread id`，`callback_id`和我们希望传入回调的数据
    ThreadPool((usize, usize, Js)),
    /// 基于epoll的事件循环所产生的事件，持有对应事件的`event_id`
    Epoll(usize),
    Timeout,
}
/// Think of this function as the javascript program you have written
fn javascript() {
    println!("First call to read test.txt");
    Fs::read("test.txt", |result| {
        let text = result.into_string().unwrap();
        let len = text.len();
        println!("First count: {} characters.", len);

        println!(r#"I want to create a "magic" number based on the text."#);
        Crypto::encrypt(text.len(), |result| {
            let n = result.into_int().unwrap();
            println!(r#""Encrypted" number is: {}"#, n);
        })
    });

    println!("Registering immediate timeout 1");
    set_timeout(0, |_res| {
        println!("Immediate1 timed out");
    });
    println!("Registering immediate timeout 2");
    set_timeout(0, |_res| {
        println!("Immediate2 timed out");
    });

    // let's read the file again and display the text
    println!("Second call to read test.txt");
    Fs::read("test.txt", |result| {
        let text = result.into_string().unwrap();
        let len = text.len();
        println!("Second count: {} characters.", len);

        // aaand one more time but not in parallel.
        println!("Third call to read test.txt");
        Fs::read("test.txt", |result| {
            let text = result.into_string().unwrap();
            println!( "file read,{}",&text,);
        });
    });

    println!("Registering a 3000 and a 500 ms timeout");
    set_timeout(3000, |_res| {
        println!("3000ms timer timed out");
        set_timeout(500, |_res| {
            println!("500ms timer(nested) timed out");
        });
    });

    println!("Registering a 1000 ms timeout");
    set_timeout(1000, |_res| {
        println!("SETTIMEOUT");
    });

    // `http_get_slow` let's us define a latency we want to simulate
    println!("Registering http get request to google.com");
    Http::http_get_slow("http//www.google.com", 2000, |result| {
        let result = result.into_string().unwrap();
        println!( "web call {}",result.trim());
    });
}
pub fn set_timeout(ms: u64, cb: impl Fn(Js) + 'static) {
    let rt = unsafe { &mut *(RUNTIME as *mut Runtime) };
    rt.set_timeout(ms, cb);
}
struct Fs;
impl Fs {
    fn read(path: &'static str, cb: impl Fn(Js) + 'static) {
        let work = move || {
            // Let's simulate that there is a very large file we're reading allowing us to actually
            // observe how the code is executed
            thread::sleep(std::time::Duration::from_secs(1));
            let mut buffer = String::new();
            fs::File::open(&path)
                .unwrap()
                .read_to_string(&mut buffer)
                .unwrap();
            Js::String(buffer)
        };
        let rt = unsafe { &mut *RUNTIME };
        rt.register_event_threadpool(work, ThreadPoolTaskKind::FileRead, cb);
    }
}
struct Crypto;
impl Crypto {
    fn encrypt(n: usize, cb: impl Fn(Js) + 'static + Clone) {
        let work = move || {
            fn fibonacchi(n: usize) -> usize {
                match n {
                    0 => 0,
                    1 => 1,
                    _ => fibonacchi(n - 1) + fibonacchi(n - 2),
                }
            }

            let fib = fibonacchi(n);
            Js::Int(fib)
        };

        let rt = unsafe { &mut *RUNTIME };
        rt.register_event_threadpool(work, ThreadPoolTaskKind::Encrypt, cb);
    }
}
struct Http;
impl Http {
    pub fn http_get_slow(url: &str, delay_ms: u32, cb: impl Fn(Js) + 'static + Clone) {
        let rt: &mut Runtime = unsafe { &mut *RUNTIME };
        // Don't worry, http://slowwly.robertomurray.co.uk is a site for simulating a delayed
        // response from a server. Perfect for our use case.
        let adr = "slowwly.robertomurray.co.uk:80";
        let mut stream = minimio::TcpStream::connect(adr).unwrap();

        let request = format!(
            "GET /delay/{}/url/http://{} HTTP/1.1\r\n\
             Host: slowwly.robertomurray.co.uk\r\n\
             Connection: close\r\n\
             \r\n",
            delay_ms, url
        );

        stream
            .write_all(request.as_bytes())
            .expect("Error writing to stream");

        let token = rt.generate_cb_identity();
        rt.epoll_registrator
            .register(&mut stream, token, minimio::Interests::READABLE)
            .unwrap();

        let wrapped = move |_n| {
            let mut stream = stream;
            let mut buffer = String::new();
            stream
                .read_to_string(&mut buffer)
                .expect("Stream read error");
            cb(Js::String(buffer));
        };

        rt.register_event_epoll(token, wrapped);
    }
}

fn main() {
    let rt = Runtime::new();
    rt.run(javascript);
}
