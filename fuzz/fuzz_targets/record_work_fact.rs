#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = mansio_server::fuzz::work_fact(data);
});
