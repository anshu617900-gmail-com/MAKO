use core::alloc::{GlobalAlloc, Layout};
use core::sync::atomic::{AtomicUsize, Ordering};
use serde::{Deserialize, Serialize};

struct BumpAllocator {
    heap: [u8; 1024 * 1024],
    next: AtomicUsize,
}

#[global_allocator]
static GLOBAL: BumpAllocator = BumpAllocator {
    heap: [0; 1024 * 1024],
    next: AtomicUsize::new(0),
};

unsafe impl GlobalAlloc for BumpAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let align = layout.align();
        let size = layout.size();
        let mut next = self.next.load(Ordering::Relaxed);
        loop {
            let aligned = (next + align - 1) & !(align - 1);
            let end = aligned + size;
            if end > self.heap.len() {
                return core::ptr::null_mut();
            }
            match self.next.compare_exchange_weak(next, end, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return self.heap.as_ptr().add(aligned) as *mut u8,
                Err(actual) => next = actual,
            }
        }
    }

    unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {
    }
}

#[derive(Deserialize)]
struct InputJson {
    message: String,
    count: u64,
}

#[derive(Serialize)]
struct OutputJson {
    message: String,
    count: u64,
    edge_node_processed: bool,
    timestamp: u64,
    transformed_count: u64,
}

#[no_mangle]
pub extern "C" fn alloc(size: usize) -> *mut u8 {
    let layout = Layout::from_size_align(size, 8).unwrap();
    unsafe { GLOBAL.alloc(layout) }
}

#[no_mangle]
pub extern "C" fn dealloc(ptr: *mut u8, size: usize) {
    let layout = Layout::from_size_align(size, 8).unwrap();
    unsafe { GLOBAL.dealloc(ptr, layout) }
}

#[no_mangle]
pub extern "C" fn transform_json(ptr: *mut u8, len: usize) -> u64 {
    let input_bytes = unsafe { core::slice::from_raw_parts(ptr, len) };
    let input_str = match core::str::from_utf8(input_bytes) {
        Ok(s) => s,
        Err(_) => return 0,
    };

    let input: InputJson = match serde_json::from_str(input_str) {
        Ok(v) => v,
        Err(_) => return 0,
    };

    let output = OutputJson {
        message: input.message,
        count: input.count,
        edge_node_processed: true,
        timestamp: 1_700_000_000,
        transformed_count: input.count * 2,
    };

    let output_str = match serde_json::to_string(&output) {
        Ok(s) => s,
        Err(_) => return 0,
    };

    let output_bytes = output_str.as_bytes();
    let out_len = output_bytes.len();

    let out_ptr = alloc(out_len);
    if out_ptr.is_null() {
        return 0;
    }

    unsafe {
        core::ptr::copy_nonoverlapping(output_bytes.as_ptr(), out_ptr, out_len);
    }

    ((out_ptr as u64) << 32) | (out_len as u64)
}