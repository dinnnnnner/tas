use libloading::Library;
use std::env;
use std::ffi::{CString, c_char};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use thiserror::Error;
use windows_sys::Win32::System::LibraryLoader::SetDllDirectoryW;

const APP_NAME: &str = "demo2_can_ingress";
const HW_TYPE_TS_USB_DEVICE: i32 = 3;
const HW_SUBTYPE_TC1012: i32 = 12;
const APP_CAN: i32 = 0;
const CANFD_TYPE_ISO: i32 = 1;
const CANFD_MODE_NORMAL: i32 = 0;
const CALLBACK_QUEUE_CAPACITY: usize = 4096;
const MASK_CAN_PROP_DIR_TX: u8 = 0x01;
const MASK_CAN_PROP_EXTEND: u8 = 0x04;

static CALLBACK_SENDERS: OnceLock<Mutex<Vec<(u64, SyncSender<CanFrame>)>>> = OnceLock::new();
static CALLBACK_SENDER_SEQ: AtomicU64 = AtomicU64::new(1);
static TSMASTER_AUTOSTART_ATTEMPTED: AtomicBool = AtomicBool::new(false);

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct TLibCan {
    idx_chn: u8,
    properties: u8,
    dlc: u8,
    reserved: u8,
    identifier: i32,
    time_us: u64,
    data: [u8; 8],
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct TLibCanFd {
    idx_chn: u8,
    properties: u8,
    dlc: u8,
    flags: u8,
    identifier: i32,
    time_us: u64,
    data: [u8; 64],
}

type CanCallback = unsafe extern "system" fn(*mut i32, *const TLibCan);
type CanFdCallback = unsafe extern "system" fn(*mut i32, *const TLibCanFd);
type SetLibLocation = unsafe extern "system" fn(*const c_char) -> i32;
type InitializeLib = unsafe extern "system" fn(*const c_char) -> i32;
type FinalizeLib = unsafe extern "system" fn();
type SetCanChannelCount = unsafe extern "system" fn(i32) -> i32;
type SetMappingVerbose =
    unsafe extern "system" fn(*const c_char, i32, i32, *const c_char, i32, i32, i32, i32, i32) -> i32;
type ConfigureBaudrateCanFd = unsafe extern "system" fn(i32, f32, f32, i32, i32, i32) -> i32;
type AppConnect = unsafe extern "system" fn() -> i32;
type AppDisconnect = unsafe extern "system" fn() -> i32;
type RegisterEventCan = unsafe extern "system" fn(*mut i32, CanCallback) -> i32;
type UnregisterEventCan = unsafe extern "system" fn(*mut i32, CanCallback) -> i32;
type RegisterEventCanFd = unsafe extern "system" fn(*mut i32, CanFdCallback) -> i32;
type UnregisterEventCanFd = unsafe extern "system" fn(*mut i32, CanFdCallback) -> i32;
type TransmitCanAsync = unsafe extern "system" fn(*const TLibCan) -> i32;
type TransmitCanFdAsync = unsafe extern "system" fn(*const TLibCanFd) -> i32;

#[derive(Clone, Debug)]
pub struct CanTransportConfig {
    pub tsmaster_bin: Option<PathBuf>,
    pub autostart_tsmaster: bool,
    pub hardware_name: String,
    pub channel_index: u8,
    pub channel_count: i32,
    pub arbitration_baud_kbps: u32,
    pub data_baud_kbps: u32,
    pub receive_timeout: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct CanFrame {
    pub channel: u8,
    pub properties: u8,
    pub dlc: u8,
    pub identifier: u32,
    pub timestamp_us: u64,
    pub data: [u8; 64],
}

#[derive(Clone, Copy, Debug)]
pub struct CanTxFrame {
    pub channel: u8,
    pub identifier: u32,
    pub is_extended: bool,
    pub dlc: u8,
    pub data: [u8; 64],
}

#[derive(Debug, Error)]
pub enum CanTransportError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("not connected")]
    NotConnected,
    #[error("transport lock poisoned")]
    LockPoisoned,
    #[error("invalid c string: {0}")]
    InvalidCString(#[from] std::ffi::NulError),
    #[error("{name} failed: {code}")]
    ApiCall { name: &'static str, code: i32 },
    #[error("TSMaster.dll not found: {0}")]
    DllNotFound(PathBuf),
    #[error("callback channel disconnected")]
    CallbackDisconnected,
}

struct TsMasterApi {
    _lib: Library,
    set_libtsmaster_location: SetLibLocation,
    initialize_lib_tsmaster: InitializeLib,
    finalize_lib_tsmaster: FinalizeLib,
    tsapp_set_can_channel_count: SetCanChannelCount,
    tsapp_set_mapping_verbose: SetMappingVerbose,
    tsapp_configure_baudrate_canfd: ConfigureBaudrateCanFd,
    tsapp_connect: AppConnect,
    tsapp_disconnect: AppDisconnect,
    tsapp_register_event_can: RegisterEventCan,
    tsapp_unregister_event_can: UnregisterEventCan,
    tsapp_register_event_canfd: Option<RegisterEventCanFd>,
    tsapp_unregister_event_canfd: Option<UnregisterEventCanFd>,
    tsapp_transmit_can_async: TransmitCanAsync,
    tsapp_transmit_canfd_async: Option<TransmitCanFdAsync>,
}

pub struct CanTransport {
    config: CanTransportConfig,
    api: Option<TsMasterApi>,
    rx: Option<Arc<Mutex<Receiver<CanFrame>>>>,
    event_obj: i32,
    callback_sender_id: Option<u64>,
}

impl Default for CanTransportConfig {
    fn default() -> Self {
        Self {
            tsmaster_bin: None,
            autostart_tsmaster: true,
            hardware_name: "TC1012".to_string(),
            channel_index: 0,
            channel_count: 1,
            arbitration_baud_kbps: 500,
            data_baud_kbps: 2_000,
            receive_timeout: Duration::from_millis(200),
        }
    }
}

impl CanFrame {
    pub fn data_len(&self) -> usize {
        can_dlc_len(self.dlc)
    }

    pub fn data_bytes(&self) -> &[u8] {
        &self.data[..self.data_len().min(self.data.len())]
    }

    pub fn is_tx(&self) -> bool {
        self.properties & 0x01 == 0x01
    }
}

impl CanTxFrame {
    pub fn new(channel: u8, identifier: u32, dlc: u8, data: [u8; 8]) -> Self {
        let mut full_data = [0u8; 64];
        full_data[..8].copy_from_slice(&data);
        Self {
            channel,
            identifier,
            is_extended: false,
            dlc,
            data: full_data,
        }
    }

    pub fn new_fd(channel: u8, identifier: u32, dlc: u8, data: &[u8]) -> Self {
        let mut full_data = [0u8; 64];
        let len = data.len().min(64);
        full_data[..len].copy_from_slice(&data[..len]);
        Self {
            channel,
            identifier,
            is_extended: false,
            dlc,
            data: full_data,
        }
    }

    fn to_raw(self) -> TLibCan {
        let mut properties = MASK_CAN_PROP_DIR_TX;
        if self.is_extended {
            properties |= MASK_CAN_PROP_EXTEND;
        }
        TLibCan {
            idx_chn: self.channel,
            properties,
            dlc: self.dlc.min(8),
            reserved: 0,
            identifier: self.identifier as i32,
            time_us: 0,
            data: self.data[..8].try_into().unwrap_or([0u8; 8]),
        }
    }

    fn to_raw_fd(self) -> TLibCanFd {
        let mut properties = MASK_CAN_PROP_DIR_TX;
        if self.is_extended {
            properties |= MASK_CAN_PROP_EXTEND;
        }
        TLibCanFd {
            idx_chn: self.channel,
            properties,
            dlc: self.dlc,
            flags: 1,
            identifier: self.identifier as i32,
            time_us: 0,
            data: self.data,
        }
    }
}

fn can_dlc_len(dlc: u8) -> usize {
    match dlc {
        0..=8 => usize::from(dlc),
        9 => 12,
        10 => 16,
        11 => 20,
        12 => 24,
        13 => 32,
        14 => 48,
        _ => 64,
    }
}

impl TsMasterApi {
    unsafe fn load(dll_path: &Path) -> Result<Self, CanTransportError> {
        let lib = unsafe { Library::new(dll_path) }
            .map_err(|err| io::Error::other(err.to_string()))?;
        let set_libtsmaster_location = *unsafe {
            lib.get::<SetLibLocation>(b"set_libtsmaster_location\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let initialize_lib_tsmaster = *unsafe {
            lib.get::<InitializeLib>(b"initialize_lib_tsmaster\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let finalize_lib_tsmaster = *unsafe {
            lib.get::<FinalizeLib>(b"finalize_lib_tsmaster\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_set_can_channel_count = *unsafe {
            lib.get::<SetCanChannelCount>(b"tsapp_set_can_channel_count\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_set_mapping_verbose = *unsafe {
            lib.get::<SetMappingVerbose>(b"tsapp_set_mapping_verbose\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_configure_baudrate_canfd = *unsafe {
            lib.get::<ConfigureBaudrateCanFd>(b"tsapp_configure_baudrate_canfd\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_connect = *unsafe {
            lib.get::<AppConnect>(b"tsapp_connect\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_disconnect = *unsafe {
            lib.get::<AppDisconnect>(b"tsapp_disconnect\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_register_event_can = *unsafe {
            lib.get::<RegisterEventCan>(b"tsapp_register_event_can\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_unregister_event_can = *unsafe {
            lib.get::<UnregisterEventCan>(b"tsapp_unregister_event_can\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_register_event_canfd = unsafe {
            lib.get::<RegisterEventCanFd>(b"tsapp_register_event_canfd\0")
                .ok()
                .map(|symbol| *symbol)
        };
        let tsapp_unregister_event_canfd = unsafe {
            lib.get::<UnregisterEventCanFd>(b"tsapp_unregister_event_canfd\0")
                .ok()
                .map(|symbol| *symbol)
        };
        let tsapp_transmit_can_async = *unsafe {
            lib.get::<TransmitCanAsync>(b"tsapp_transmit_can_async\0")
                .map_err(|err| io::Error::other(err.to_string()))?
        };
        let tsapp_transmit_canfd_async = unsafe {
            lib.get::<TransmitCanFdAsync>(b"tsapp_transmit_canfd_async\0")
                .ok()
                .map(|symbol| *symbol)
        };

        Ok(Self {
            _lib: lib,
            set_libtsmaster_location,
            initialize_lib_tsmaster,
            finalize_lib_tsmaster,
            tsapp_set_can_channel_count,
            tsapp_set_mapping_verbose,
            tsapp_configure_baudrate_canfd,
            tsapp_connect,
            tsapp_disconnect,
            tsapp_register_event_can,
            tsapp_unregister_event_can,
            tsapp_register_event_canfd,
            tsapp_unregister_event_canfd,
            tsapp_transmit_can_async,
            tsapp_transmit_canfd_async,
        })
    }
}

impl CanTransport {
    pub fn new(config: CanTransportConfig) -> Self {
        Self {
            config,
            api: None,
            rx: None,
            event_obj: 0,
            callback_sender_id: None,
        }
    }

    pub async fn connect(&mut self) -> Result<(), CanTransportError> {
        let runtime_dir = resolve_tsmaster_bin(self.config.tsmaster_bin.clone());
        if self.config.autostart_tsmaster {
            maybe_autostart_tsmaster(&runtime_dir)?;
        }
        let dll_path = runtime_dir.join("TSMaster.dll");
        if !dll_path.exists() {
            return Err(CanTransportError::DllNotFound(dll_path));
        }

        let dll_dir_w = wide_null(&runtime_dir);
        unsafe {
            SetDllDirectoryW(dll_dir_w.as_ptr());
        }

        let app_name = CString::new(APP_NAME)?;
        let hw_name = CString::new(self.config.hardware_name.clone())?;
        let runtime_dir_c = CString::new(runtime_dir.to_string_lossy().as_ref())?;
        let api = unsafe { TsMasterApi::load(&dll_path)? };

        check(
            "set_libtsmaster_location",
            unsafe { (api.set_libtsmaster_location)(runtime_dir_c.as_ptr()) },
        )?;
        check(
            "initialize_lib_tsmaster",
            unsafe { (api.initialize_lib_tsmaster)(app_name.as_ptr()) },
        )?;

        let (tx, rx) = mpsc::sync_channel(CALLBACK_QUEUE_CAPACITY);
        let callback_sender_id = register_callback_sender(tx)?;

        let init_result = (|| -> Result<(), CanTransportError> {
            check(
                "tsapp_set_can_channel_count",
                unsafe { (api.tsapp_set_can_channel_count)(self.config.channel_count) },
            )?;
            check(
                "tsapp_set_mapping_verbose",
                unsafe {
                    (api.tsapp_set_mapping_verbose)(
                        app_name.as_ptr(),
                        APP_CAN,
                        i32::from(self.config.channel_index),
                        hw_name.as_ptr(),
                        HW_TYPE_TS_USB_DEVICE,
                        HW_SUBTYPE_TC1012,
                        i32::from(self.config.channel_index),
                        0,
                        1,
                    )
                },
            )?;
            check(
                "tsapp_configure_baudrate_canfd",
                unsafe {
                    (api.tsapp_configure_baudrate_canfd)(
                        i32::from(self.config.channel_index),
                        self.config.arbitration_baud_kbps as f32,
                        self.config.data_baud_kbps as f32,
                        CANFD_TYPE_ISO,
                        CANFD_MODE_NORMAL,
                        1,
                    )
                },
            )?;
            check("tsapp_connect", unsafe { (api.tsapp_connect)() })?;
            check(
                "tsapp_register_event_can",
                unsafe { (api.tsapp_register_event_can)(&mut self.event_obj, on_can_event) },
            )?;
            if let Some(register_canfd) = api.tsapp_register_event_canfd {
                check(
                    "tsapp_register_event_canfd",
                    unsafe { register_canfd(&mut self.event_obj, on_canfd_event) },
                )?;
            }
            Ok(())
        })();

        if let Err(err) = init_result {
            unregister_callback_sender(callback_sender_id)?;
            let _ = unsafe { (api.tsapp_disconnect)() };
            unsafe { (api.finalize_lib_tsmaster)() };
            return Err(err);
        }

        self.rx = Some(Arc::new(Mutex::new(rx)));
        self.api = Some(api);
        self.callback_sender_id = Some(callback_sender_id);
        Ok(())
    }

    pub async fn recv(&self) -> Result<Option<CanFrame>, CanTransportError> {
        let rx = self.rx.clone().ok_or(CanTransportError::NotConnected)?;
        let timeout = self.config.receive_timeout;
        tokio::task::spawn_blocking(move || {
            let guard = rx.lock().map_err(|_| CanTransportError::LockPoisoned)?;
            match guard.recv_timeout(timeout) {
                Ok(frame) => Ok(Some(frame)),
                Err(RecvTimeoutError::Timeout) => Ok(None),
                Err(RecvTimeoutError::Disconnected) => Err(CanTransportError::CallbackDisconnected),
            }
        })
        .await
        .map_err(join_to_io)?
    }

    pub async fn transmit(&self, frame: CanTxFrame) -> Result<(), CanTransportError> {
        let Some(api) = self.api.as_ref() else {
            return Err(CanTransportError::NotConnected);
        };
        if frame.dlc > 8 {
            let Some(transmit_canfd) = api.tsapp_transmit_canfd_async else {
                return Err(CanTransportError::ApiCall {
                    name: "tsapp_transmit_canfd_async",
                    code: -1,
                });
            };
            let raw = frame.to_raw_fd();
            check("tsapp_transmit_canfd_async", unsafe { transmit_canfd(&raw) })?;
        } else {
            let raw = frame.to_raw();
            check(
                "tsapp_transmit_can_async",
                unsafe { (api.tsapp_transmit_can_async)(&raw) },
            )?;
        }
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), CanTransportError> {
        if let Some(api) = self.api.take() {
            if let Some(unregister_canfd) = api.tsapp_unregister_event_canfd {
                let _ = unsafe { unregister_canfd(&mut self.event_obj, on_canfd_event) };
            }
            let _ = unsafe { (api.tsapp_unregister_event_can)(&mut self.event_obj, on_can_event) };
            let _ = unsafe { (api.tsapp_disconnect)() };
            unsafe { (api.finalize_lib_tsmaster)() };
        }
        self.rx = None;
        self.event_obj = 0;
        if let Some(id) = self.callback_sender_id.take() {
            unregister_callback_sender(id)?;
        }
        Ok(())
    }
}

impl Drop for CanTransport {
    fn drop(&mut self) {
        let _ = self.api.take().map(|api| {
            if let Some(unregister_canfd) = api.tsapp_unregister_event_canfd {
                let _ = unsafe { unregister_canfd(&mut self.event_obj, on_canfd_event) };
            }
            let _ = unsafe { (api.tsapp_unregister_event_can)(&mut self.event_obj, on_can_event) };
            let _ = unsafe { (api.tsapp_disconnect)() };
            unsafe { (api.finalize_lib_tsmaster)() };
        });
        if let Some(id) = self.callback_sender_id.take() {
            let _ = unregister_callback_sender(id);
        }
    }
}

unsafe extern "system" fn on_can_event(_: *mut i32, frame: *const TLibCan) {
    if frame.is_null() {
        return;
    }

    let sender = CALLBACK_SENDERS.get_or_init(|| Mutex::new(Vec::new()));
    let Ok(guard) = sender.lock() else {
        return;
    };
    if guard.is_empty() {
        return;
    }

    let raw = unsafe { std::ptr::read_unaligned(frame) };
    let parsed = CanFrame {
        channel: raw.idx_chn,
        properties: raw.properties,
        dlc: raw.dlc,
        identifier: raw.identifier as u32,
        timestamp_us: raw.time_us,
        data: {
            let mut data = [0u8; 64];
            data[..8].copy_from_slice(&raw.data);
            data
        },
    };
    for (_, tx) in guard.iter() {
        let _ = tx.try_send(parsed);
    }
}

unsafe extern "system" fn on_canfd_event(_: *mut i32, frame: *const TLibCanFd) {
    if frame.is_null() {
        return;
    }

    let sender = CALLBACK_SENDERS.get_or_init(|| Mutex::new(Vec::new()));
    let Ok(guard) = sender.lock() else {
        return;
    };
    if guard.is_empty() {
        return;
    }

    let raw = unsafe { std::ptr::read_unaligned(frame) };
    let parsed = CanFrame {
        channel: raw.idx_chn,
        properties: raw.properties,
        dlc: raw.dlc,
        identifier: raw.identifier as u32,
        timestamp_us: raw.time_us,
        data: raw.data,
    };
    for (_, tx) in guard.iter() {
        let _ = tx.try_send(parsed);
    }
}

fn resolve_tsmaster_bin(configured: Option<PathBuf>) -> PathBuf {
    if let Some(path) = configured {
        return path;
    }
    if let Ok(path) = env::var("TSMASTER_BIN") {
        return PathBuf::from(path);
    }
    PathBuf::from(r"D:\TSMaster\bin64")
}

fn maybe_autostart_tsmaster(runtime_dir: &Path) -> Result<(), CanTransportError> {
    if TSMASTER_AUTOSTART_ATTEMPTED.swap(true, Ordering::Relaxed) {
        return Ok(());
    }

    let Some(exe_path) = tsmaster_exe_path(runtime_dir) else {
        return Ok(());
    };

    Command::new(&exe_path)
        .current_dir(exe_path.parent().unwrap_or(runtime_dir))
        .spawn()?;

    std::thread::sleep(Duration::from_millis(1_500));
    Ok(())
}

fn tsmaster_exe_path(runtime_dir: &Path) -> Option<PathBuf> {
    let direct = runtime_dir.join("TSMaster.exe");
    if direct.exists() {
        return Some(direct);
    }

    let parent = runtime_dir.parent()?.join("TSMaster.exe");
    if parent.exists() {
        return Some(parent);
    }

    None
}

fn wide_null(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain([0])
        .collect()
}

fn check(name: &'static str, code: i32) -> Result<(), CanTransportError> {
    if code == 0 {
        Ok(())
    } else {
        Err(CanTransportError::ApiCall { name, code })
    }
}

fn join_to_io(err: tokio::task::JoinError) -> CanTransportError {
    CanTransportError::Io(io::Error::other(err.to_string()))
}

fn register_callback_sender(sender: SyncSender<CanFrame>) -> Result<u64, CanTransportError> {
    let id = CALLBACK_SENDER_SEQ.fetch_add(1, Ordering::Relaxed);
    let slot = CALLBACK_SENDERS.get_or_init(|| Mutex::new(Vec::new()));
    let mut guard = slot.lock().map_err(|_| CanTransportError::LockPoisoned)?;
    guard.push((id, sender));
    Ok(id)
}

fn unregister_callback_sender(id: u64) -> Result<(), CanTransportError> {
    let slot = CALLBACK_SENDERS.get_or_init(|| Mutex::new(Vec::new()));
    let mut guard = slot.lock().map_err(|_| CanTransportError::LockPoisoned)?;
    guard.retain(|(existing_id, _)| *existing_id != id);
    Ok(())
}
