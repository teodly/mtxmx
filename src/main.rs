use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::{sync::atomic::AtomicBool, time::Duration};
use std::sync::atomic::Ordering;

use atomic_float::AtomicF32;
use clap::Parser;
use itertools::Itertools;
use log::error;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};


const ABOUT: &str = "MTXMX - a matrix mixer
Copyright (C) 2025 Teodor Wozniak

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
";

struct Matrix<T> {
    flat: Vec<T>,
    outputs_count: usize,
    inputs_count: usize,
}

impl<T: Default> Matrix<T> {
    pub fn new(outputs_count: usize, inputs_count: usize) -> Self {
        Self {
            flat: (0..outputs_count * inputs_count).map(|_|T::default()).collect(),
            outputs_count,
            inputs_count,
        }
    }
    #[inline(always)]
    fn flatten_index(&self, output_index: usize, input_index: usize) -> usize {
        debug_assert!(output_index < self.outputs_count);
        debug_assert!(input_index < self.inputs_count);
        output_index * self.inputs_count + input_index
    }
    #[inline(always)]
    pub fn cell(&self, output_index: usize, input_index: usize) -> &T {
        &self.flat[self.flatten_index(output_index, input_index)]
    }
    #[inline(always)]
    pub fn cell_mut(&mut self, output_index: usize, input_index: usize) -> &mut T {
        let index = self.flatten_index(output_index, input_index);
        &mut self.flat[index]
    }
    #[inline(always)]
    pub fn for_each_output(&self, input_index: usize, mut cb: impl FnMut(&T)) {
        for i in 0..self.outputs_count {
            cb(self.cell(i, input_index));
        }
    }
    #[inline(always)]
    pub fn for_each_input(&self, output_index: usize, mut cb: impl FnMut(&T)) {
        for i in 0..self.inputs_count {
            cb(self.cell(output_index, i));
        }
    }
}

fn print_matrix(matrix: &Matrix<AtomicF32>) {
    println!("---matrix---");
    for out_index in 0..matrix.outputs_count {
        let line = (0..matrix.inputs_count).map(|in_index| {
            let v = matrix.cell(out_index, in_index).load(Ordering::Relaxed);
            format!("{v:>4} ")
        }).collect::<String>();
        println!("{line}");
    }
    println!("---end of matrix---");
}

struct RealTimeMixer {
    matrix: Matrix<AtomicF32>,
    //matrix_meters: Matrix<AtomicF32>,
    input_meters: Vec<AtomicF32>,
    active_outputs: Vec<AtomicBool>,
    active_inputs: Vec<AtomicBool>,
}

struct RealTimeInternal {
    output_ports: RefCell<Vec<jack::Port<jack::AudioOut>>>,
    input_ports: Vec<jack::Port<jack::AudioIn>>,
}

impl RealTimeMixer {
    pub fn set_level(&self, output_index: usize, input_index: usize, level: f32) {
        self.matrix.cell(output_index, input_index).store(level, Ordering::Relaxed);
    }
    pub fn get_level(&self, output_index: usize, input_index: usize) -> f32 {
        self.matrix.cell(output_index, input_index).load(Ordering::Relaxed)
    }
    /* fn zip_enum_filter<T>(elems: impl IntoIterator<Item=T>, are_active: impl IntoIterator<Item=bool>) -> _ {
        
    } */
    #[inline(always)]
    fn process(&self, internal: &mut RealTimeInternal, _client: &jack::Client, ps: &jack::ProcessScope) {
        for (output_index, (output_port, is_active)) in internal.output_ports.borrow_mut().iter_mut().zip_eq(&self.active_outputs).enumerate() {
            let output_slice = output_port.as_mut_slice(ps);
            output_slice.fill(0.0);
            if !is_active.load(Ordering::Relaxed) {
                continue;
            }
            
            for (input_index, input_port) in internal.input_ports.iter().zip_eq(&self.active_inputs).enumerate().filter(|(_, (_, a))|a.load(Ordering::Relaxed)).map(|(i, (p, _))|(i, p)) {
                let multiplier = self.get_level(output_index, input_index);
                let input_slice = input_port.as_slice(ps); // TODO cache
                //let mut peak = 0.0f32;
                for i in 0..ps.n_frames() as usize {
                    let sample = input_slice[i] * multiplier;
                    //peak = peak.max(sample.abs());
                    output_slice[i] += sample;
                }
                //self.matrix_meters.cell(output_index, input_index).fetch_max(peak, Ordering::Relaxed);
            }
        }
        for (input_index, input_port) in internal.input_ports.iter().zip_eq(&self.active_inputs).enumerate().filter(|(_, (_, a))|a.load(Ordering::Relaxed)).map(|(i, (p, _))|(i, p)) {
            self.input_meters[input_index].fetch_max(input_port.as_slice(ps).iter().map(|v|v.abs()).reduce(f32::max).unwrap(), Ordering::Relaxed);
        }
    }
}

const MAX_CHANNELS_PER_ENDPOINT: usize = 16;

struct Endpoint {
    pub name: String,
    pub gain: f32,
    pub connect_to: Vec<String>,
    pub rt_channels: Vec<usize>,
}

struct MatrixPoint {
    enabled: bool,
    level: f32,
}

impl Default for MatrixPoint {
    fn default() -> Self {
        Self { enabled: false, level: 0.0 }
    }
}


fn db_to_lin(db: f32) -> f32 {
    10.0f32.powf(db/20.0)
}
fn lin_to_db(lin: f32) -> f32 {
    20.0 * f32::log10(lin)
}

struct HighLevelMixer<N, P> {
    outputs: Vec<Endpoint>,
    inputs: Vec<Endpoint>,
    rt_outputs: Vec<jack::Port<jack::Unowned>>,
    rt_inputs: Vec<jack::Port<jack::Unowned>>,
    matrix: Matrix<MatrixPoint>,
    rt: Arc<RealTimeMixer>,
    jack_client: jack::AsyncClient<N, P>
}

impl<N, P> HighLevelMixer<N, P> {
    fn on_topic_update(&mut self, parts: Vec<&str>, value: &str) -> bool {
        log::info!("on_topic_update {parts:?}");
        if parts.len() >= 2 {
            if let Some(out_id) = parts[0].strip_prefix("out").map(|s|s.parse::<usize>().ok()).flatten() {
                if let Some(in_id) = parts[1].strip_prefix("in").map(|s|s.parse::<usize>().ok()).flatten() {
                    if parts.len() == 3 {
                        if out_id < 1 || in_id < 1 {
                            return false;
                        }
                        let out_index = out_id-1;
                        let in_index = in_id-1;
                        if out_index >= self.outputs.len() || in_index >= self.inputs.len() {
                            return false;
                        }
                        match parts[2] {
                            "state" => {
                                let is_on = value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("on");
                                let is_off = value.eq_ignore_ascii_case("false") || value.eq_ignore_ascii_case("off");
                                if !(is_on || is_off) {
                                    return false;
                                }
                                self.matrix.cell_mut(out_index, in_index).enabled = is_on;
                                self.commit_point(out_index, in_index);
                                print_matrix(&self.rt.matrix);
                                return true;
                            }
                            "level" => {
                                if let Ok(new_level) = value.parse::<f32>() {
                                    self.matrix.cell_mut(out_index, in_index).level = db_to_lin(new_level);
                                    self.commit_point(out_index, in_index);
                                    print_matrix(&self.rt.matrix);
                                    return true;
                                }
                            }
                            _ => {
                                return false;
                            }
                        }
                    }
                }
            }
            if parts[0]=="config" {
                let out_id_opt = parts[1].strip_prefix("out").map(|s|s.parse::<usize>().ok()).flatten();
                let in_id_opt = parts[1].strip_prefix("in").map(|s|s.parse::<usize>().ok()).flatten();

                let endpoint = if let Some(out_id) = out_id_opt {
                    if out_id < 1 || out_id > self.outputs.len() { return false; }
                    Some(&mut self.outputs[out_id-1])
                } else if let Some(in_id) = in_id_opt {
                    if in_id < 1 || in_id > self.inputs.len() { return false; }
                    Some(&mut self.inputs[in_id-1])
                } else {
                    None
                };
                
                if let Some(endpoint) = endpoint {
                    if parts.len() == 3 {
                        match parts[2] {
                            "gain" => {
                                if let Ok(new_level) = value.parse::<f32>() {
                                    endpoint.gain = db_to_lin(new_level);
                                    if let Some(out_id) = out_id_opt {
                                        self.commit_output(out_id-1);
                                    } else if let Some(in_id) = in_id_opt {
                                        self.commit_input(in_id-1);
                                    }
                                    print_matrix(&self.rt.matrix);
                                    return true;
                                }
                            },
                            "name" => {
                                endpoint.name = value.to_owned();
                                if let Some(out_id) = out_id_opt {
                                    self.update_output(out_id-1);
                                } else if let Some(in_id) = in_id_opt {
                                    self.update_input(in_id-1);
                                }
                                return true;
                            },
                            "connect_to" => {
                                if let Ok(jvec) = serde_json::from_str::<Vec<String>>(value) {
                                    if jvec.len() > MAX_CHANNELS_PER_ENDPOINT {
                                        return false;
                                        //jvec.resize(MAX_CHANNELS_PER_ENDPOINT, "".into());
                                    }
                                    if let Some(out_id) = out_id_opt {
                                        let out_index = out_id-1;
                                        self.disable_output(out_index, true);
                                        self.outputs[out_index].connect_to = jvec;
                                        self.enable_output(out_index);
                                        return true;
                                    } else if let Some(in_id) = in_id_opt {
                                        let in_index = in_id-1;
                                        self.disable_input(in_index, true);
                                        self.inputs[in_index].connect_to = jvec;
                                        self.enable_input(in_index);
                                        return true;
                                    }
                                }
                            },
                            _ => {
                                return false;
                            }
                        }
                    }
                }
            }
        }
        false
    }
    fn commit_point(&self, out_index: usize, in_index: usize) {
        let output = &self.outputs[out_index];
        let input = &self.inputs[in_index];
        if output.rt_channels.is_empty() || input.rt_channels.is_empty() {
            return;
        }
        let point = self.matrix.cell(out_index, in_index);
        let level = if point.enabled { point.level * input.gain * output.gain } else { 0.0 };
        if output.rt_channels.len() == input.rt_channels.len() {
            // stereo -> stereo: identity matrix
            for (rt_out_index, rt_in_index) in itertools::zip_eq(&output.rt_channels, &input.rt_channels) {
                self.rt.set_level(*rt_out_index, *rt_in_index, level);
            }
        } else {
            // stereo -> mono or mono -> stereo
            let level_scaled = level / (input.rt_channels.len() as f32);
            for rt_out_index in &output.rt_channels {
                for rt_in_index in &input.rt_channels {
                    self.rt.set_level(*rt_out_index, *rt_in_index, level_scaled);
                }
            }
        }
    }
    fn commit_output(&self, index: usize) {
        for i in 0..self.inputs.len() {
            self.commit_point(index, i);
        }
    }
    fn commit_input(&self, index: usize) {
        for i in 0..self.outputs.len() {
            self.commit_point(i, index);
        }
    }
    fn disable_output(&mut self, index: usize, clear_matrix: bool) {
        for &chi in &self.outputs[index].rt_channels {
            if clear_matrix {
                self.rt.matrix.for_each_input(chi, |point|point.store(0.0, Ordering::Relaxed));
            }
            for conn in self.rt_outputs[chi].get_connections() {
                let _ = self.jack_client.as_client().disconnect_ports_by_name(&self.rt_outputs[chi].name().unwrap(), &conn);
            }
            self.rt_outputs[chi].set_name(&format!("unused_o{chi:04}")).unwrap();
            self.rt.active_outputs[chi].store(false, Ordering::Relaxed);
        }
        self.outputs[index].connect_to.clear();
        self.outputs[index].rt_channels.clear();
    }
    fn disable_input(&mut self, index: usize, clear_matrix: bool) {
        for &chi in &self.inputs[index].rt_channels {
            if clear_matrix {
                self.rt.matrix.for_each_output(chi, |point|point.store(0.0, Ordering::Relaxed));
            }
            for conn in self.rt_inputs[chi].get_connections() {
                let _ = self.jack_client.as_client().disconnect_ports_by_name(&conn, &self.rt_inputs[chi].name().unwrap());
            }
            self.rt_inputs[chi].set_name(&format!("unused_i{chi:04}")).unwrap();
            self.rt.active_inputs[chi].store(false, Ordering::Relaxed);
        }
        self.inputs[index].connect_to.clear();
        self.inputs[index].rt_channels.clear();
    }
    fn update_output(&mut self, index: usize) {
        let output = &self.outputs[index];
        for (ch_index, (connect_to, &rt_index)) in output.connect_to.iter().zip_eq(output.rt_channels.iter()).enumerate() {
            let port_name = format!("to_{}_{:02}", output.name, ch_index+1);
            self.rt_outputs[rt_index].set_name(&port_name).unwrap();
            let _ = self.jack_client.as_client().connect_ports_by_name(&self.rt_outputs[rt_index].name().unwrap(), connect_to);
        }
    }
    fn update_input(&mut self, index: usize) {
        let input = &self.inputs[index];
        for (ch_index, (connect_to, &rt_index)) in input.connect_to.iter().zip_eq(input.rt_channels.iter()).enumerate() {
            let port_name = format!("from_{}_{:02}", input.name, ch_index+1);
            self.rt_inputs[rt_index].set_name(&port_name).unwrap();
            let _ = self.jack_client.as_client().connect_ports_by_name(connect_to, &self.rt_inputs[rt_index].name().unwrap());
        }
    }
    fn enable_output(&mut self, index: usize) {
        let mut rt_channels = vec![];
        for _ in self.outputs[index].connect_to.iter() {
            if let Some(rt_index) = self.rt.active_outputs.iter().position(|a|!a.load(Ordering::Relaxed)) {
                rt_channels.push(rt_index);
                self.rt.active_outputs[rt_index].store(true, Ordering::Relaxed);
            }
        }
        self.outputs[index].rt_channels = rt_channels;
        self.update_output(index);
        self.commit_output(index);
    }
    fn enable_input(&mut self, index: usize) {
        let mut rt_channels = vec![];
        for _ in self.inputs[index].connect_to.iter() {
            if let Some(rt_index) = self.rt.active_inputs.iter().position(|a|!a.load(Ordering::Relaxed)) {
                rt_channels.push(rt_index);
                self.rt.active_inputs[rt_index].store(true, Ordering::Relaxed);
            }
        }
        self.inputs[index].rt_channels = rt_channels;
        self.update_input(index);
        self.commit_input(index);
    }
    fn rescan_ports(&mut self) {
        for i in 0..self.outputs.len() {
            self.update_output(i);
        }
        for i in 0..self.inputs.len() {
            self.update_input(i);
        }
    }
}


struct ToMQTT {
    topic: String,
    value: String,
}

struct JackNotifications {
    xrun_counter: usize,
    to_mqtt: tokio::sync::mpsc::Sender<ToMQTT>,
    rescan: tokio::sync::mpsc::Sender<()>,
}

impl jack::NotificationHandler for JackNotifications {
    fn thread_init(&self, _: &jack::Client) {
    }

    /// Not much we can do here, see https://man7.org/linux/man-pages/man7/signal-safety.7.html.
    unsafe fn shutdown(&mut self, _: jack::ClientStatus, _: &str) {}

    fn freewheel(&mut self, _: &jack::Client, _is_enabled: bool) {
    }

    fn sample_rate(&mut self, _: &jack::Client, srate: jack::Frames) -> jack::Control {
        log::info!("JACK: sample rate changed to {srate}");
        let _ = self.to_mqtt.try_send(ToMQTT { topic: "status/sample_rate".to_owned(), value: srate.to_string() });
        jack::Control::Continue
    }

    fn client_registration(&mut self, _: &jack::Client, _name: &str, _is_reg: bool) {
    }

    fn port_registration(&mut self, _: &jack::Client, _port_id: jack::PortId, is_reg: bool) {
        if is_reg {
            let _ = self.rescan.try_send(());
        }
    }

    fn port_rename(&mut self, _: &jack::Client, _port_id: jack::PortId, _old_name: &str, _new_name: &str) -> jack::Control {
        let _ = self.rescan.try_send(());
        jack::Control::Continue
    }

    fn ports_connected(&mut self, _: &jack::Client, _port_id_a: jack::PortId, _port_id_b: jack::PortId, _are_connected: bool) {
    }

    fn graph_reorder(&mut self, _: &jack::Client) -> jack::Control {
        jack::Control::Continue
    }

    fn xrun(&mut self, _: &jack::Client) -> jack::Control {
        log::info!("JACK: xrun occurred");
        self.xrun_counter += 1;
        let _ = self.to_mqtt.try_send(ToMQTT { topic: "status/xruns".to_owned(), value: self.xrun_counter.to_string() });
        jack::Control::Continue
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about = ABOUT, long_about = None, arg_required_else_help = true)]
struct Args {
    #[arg(long, short)]
    url: String,
    #[arg(long, short, default_value("mtxmx"))]
    root: String,
    #[arg(long, short, default_value("mtxmx"))]
    jack_client: String,
    #[arg(long, default_value("16"))]
    input_channels_max: usize,
    #[arg(long, default_value("16"))]
    output_channels_max: usize,
    #[arg(long, default_value("16"))]
    input_endpoints_max: usize,
    #[arg(long, default_value("16"))]
    output_endpoints_max: usize,
}


#[tokio::main(flavor = "current_thread")]
async fn main() {
    let logenv = env_logger::Env::default().default_filter_or("info");
    env_logger::init_from_env(logenv);
  
    let args = Args::parse();

    let mut mqttoptions = MqttOptions::parse_url(args.url).expect("failed to parse mqtt url");
    mqttoptions.set_keep_alive(Duration::from_secs(2));

    let (control_client, mut eventloop) = AsyncClient::new(mqttoptions, 100);
    control_client.subscribe(args.root.clone() + "/#", QoS::AtLeastOnce).await.expect("failed to subscribe");
    let prefix = if args.root.is_empty() { "".to_owned() } else { args.root + "/" };

    jack::set_logger(jack::LoggerType::Stdio);
    let (jack_client, _status) =
    jack::Client::new(&args.jack_client, jack::ClientOptions::default()).unwrap();

    let rt_mixer = Arc::new(RealTimeMixer {
        matrix: Matrix::new(args.output_channels_max, args.input_channels_max),
        //matrix_meters: Matrix::new(args.output_channels_max, args.input_channels_max),
        input_meters: (0..args.input_channels_max).map(|_|0.0.into()).collect(),
        active_outputs: (0..args.output_channels_max).map(|_|false.into()).collect(),
        active_inputs: (0..args.input_channels_max).map(|_|false.into()).collect(),
    });
    let mut rt_internal = RealTimeInternal {
        output_ports: RefCell::new((0..args.output_channels_max).map(|i|jack_client.register_port(&format!("never_used_o{i:04}"), jack::AudioOut::default()).unwrap()).collect()),
        input_ports: (0..args.input_channels_max).map(|i|jack_client.register_port(&format!("never_used_i{i:04}"), jack::AudioIn::default()).unwrap()).collect(),
    };
    let rt_outputs = rt_internal.output_ports.get_mut().iter().map(|port|port.clone_unowned()).collect();
    let rt_inputs = rt_internal.input_ports.iter().map(|port|port.clone_unowned()).collect();


    let (to_mqtt_sender, mut to_mqtt_receiver) = tokio::sync::mpsc::channel(100);
    let (rescan_sender, mut rescan_receiver) = tokio::sync::mpsc::channel(100);

    let rt_mixer1 = rt_mixer.clone();
    let process_callback = move |client: &jack::Client, ps: &jack::ProcessScope| -> jack::Control {
        rt_mixer1.process(&mut rt_internal, client, ps);
        jack::Control::Continue
    };
    let jack_process = jack::contrib::ClosureProcessHandler::new(process_callback);
    let active_client = jack_client.activate_async(JackNotifications { xrun_counter: 0, to_mqtt: to_mqtt_sender, rescan: rescan_sender }, jack_process).unwrap();

    let mut mixer = HighLevelMixer {
        matrix: Matrix::new(args.output_endpoints_max, args.input_endpoints_max),
        outputs: (0..args.output_endpoints_max).map(|i|Endpoint { name: format!("out{:03}", i+1), gain: 1.0, connect_to: vec![], rt_channels: vec![] }).collect(),
        inputs: (0..args.input_endpoints_max).map(|i|Endpoint { name: format!("in{:03}", i+1), gain: 1.0, connect_to: vec![], rt_channels: vec![] }).collect(),
        rt_outputs,
        rt_inputs,
        rt: rt_mixer.clone(),
        jack_client: active_client,
    };
    let _ = control_client.publish_bytes(prefix.clone() + "status/sample_rate", QoS::AtLeastOnce, true, mixer.jack_client.as_client().sample_rate().to_string().into()).await;
    let _ = control_client.publish_bytes(prefix.clone() + "status/xruns", QoS::AtLeastOnce, true, "0".into()).await;

    let mut meter_updater = tokio::time::interval(Duration::from_millis(125));
    meter_updater.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut already_received = BTreeSet::new();
    loop {
        tokio::select! {
            r = eventloop.poll() => {
                match r {
                    Ok(Event::Incoming(Incoming::Publish(packet))) => {
                        if let Some(path) = packet.topic.strip_prefix(&prefix) {
                            if let Some(ack_path) = path.strip_suffix("/set") {
                                let parts: Vec<_> = ack_path.split('/').collect();
                                let ack = mixer.on_topic_update(parts, &String::from_utf8_lossy(&packet.payload));
                                if ack {
                                    let _ = control_client.publish_bytes(prefix.clone() + ack_path, QoS::AtLeastOnce, true, packet.payload).await;
                                }
                            } else if !already_received.contains(path) {
                                mixer.on_topic_update(path.split('/').collect(), &String::from_utf8_lossy(&packet.payload));
                                already_received.insert(path.to_string());
                            }
                        }
                    }
                    Ok(_) => {},
                    Err(e) => {
                        error!("connection error: {e:?}");
                        break;
                    }
                }
            },
            _ = meter_updater.tick() => {
                let input_meters: Vec<_> = mixer.rt.input_meters.iter().map(|a|a.swap(0.0, Ordering::Relaxed)).collect();
                for (output_index, output) in mixer.outputs.iter().enumerate() {
                    if output.rt_channels.is_empty() { continue; }
                    let output_id = output_index+1;
                    for (input_index, input) in mixer.inputs.iter().enumerate() {
                        if input.rt_channels.is_empty() { continue; }
                        let input_id = input_index+1;
                        let path = format!("{prefix}out{output_id:03}/in{input_id:03}/meter");
                        let point = mixer.matrix.cell(output_index, input_index);

                        let signal_level = if point.enabled {
                            let peak = input.rt_channels.iter().map(|&ch| input_meters[ch]).reduce(f32::max).unwrap();
                            peak * input.gain * point.level
                        } else {
                            0.0
                        };
                        
                        let _ = control_client.publish_bytes(path, QoS::AtMostOnce, false, lin_to_db(signal_level).to_string().into()).await;
                    }
                }
            },
            _ = rescan_receiver.recv() => {
                mixer.rescan_ports();
            }
            msg_opt = to_mqtt_receiver.recv() => {
                if let Some(msg) = msg_opt {
                    let _ = control_client.publish_bytes(prefix.clone() + &msg.topic, QoS::AtLeastOnce, true, msg.value.into()).await;
                }
            }
            
        }
    }
}
