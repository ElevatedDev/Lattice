//! Native bootstrap support for the static topology runtime.
//!
//! This crate intentionally exposes a small JNI surface instead of a general
//! native API. The calls are for worker startup and memory-placement setup only;
//! none of these functions belong in a per-message hot path.

use core::ffi::c_void;

type JInt = i32;
type JLong = i64;
type JClass = *mut c_void;
type JNIEnv = *mut c_void;

#[allow(dead_code)]
const CAP_LINUX: u64 = 1 << 0;
#[allow(dead_code)]
const CAP_AFFINITY: u64 = 1 << 1;
#[allow(dead_code)]
const CAP_CURRENT_CPU: u64 = 1 << 2;
#[allow(dead_code)]
const CAP_NUMA_QUERY: u64 = 1 << 3;
#[allow(dead_code)]
const CAP_LOCAL_MEM_POLICY: u64 = 1 << 4;
#[allow(dead_code)]
const CAP_FIRST_TOUCH: u64 = 1 << 5;

const JNI_WORD_COUNT: usize = 16;

enum CpuListSelector {
    Stride(usize),
    Group { used: usize, group: usize },
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_nativeCapabilities0(
    _env: JNIEnv,
    _class: JClass,
) -> JLong {
    platform::native_capabilities() as JLong
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_maxCpuCount0(
    _env: JNIEnv,
    _class: JClass,
) -> JInt {
    platform::max_cpu_count()
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_configuredCpuCount0(
    _env: JNIEnv,
    _class: JClass,
) -> JInt {
    platform::configured_cpu_count()
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_onlineCpuCount0(
    _env: JNIEnv,
    _class: JClass,
) -> JInt {
    platform::online_cpu_count()
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_currentCpu0(
    _env: JNIEnv,
    _class: JClass,
) -> JInt {
    platform::current_cpu()
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_currentNumaNode0(
    _env: JNIEnv,
    _class: JClass,
) -> JInt {
    platform::current_numa_node()
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_numaNodeOfCpu0(
    _env: JNIEnv,
    _class: JClass,
    cpu: JInt,
) -> JInt {
    platform::numa_node_of_cpu(cpu)
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_pinCurrentThreadToCpu0(
    _env: JNIEnv,
    _class: JClass,
    cpu: JInt,
) -> JInt {
    platform::pin_current_thread_to_cpu(cpu)
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_pinCurrentThreadToCpuMask0(
    _env: JNIEnv,
    _class: JClass,
    word0: JLong,
    word1: JLong,
    word2: JLong,
    word3: JLong,
    word4: JLong,
    word5: JLong,
    word6: JLong,
    word7: JLong,
    word8: JLong,
    word9: JLong,
    word10: JLong,
    word11: JLong,
    word12: JLong,
    word13: JLong,
    word14: JLong,
    word15: JLong,
) -> JInt {
    platform::pin_current_thread_to_cpu_mask([
        word0, word1, word2, word3, word4, word5, word6, word7, word8, word9, word10, word11,
        word12, word13, word14, word15,
    ])
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_setLocalAllocationPolicy0(
    _env: JNIEnv,
    _class: JClass,
) -> JInt {
    platform::set_local_allocation_policy()
}

#[no_mangle]
pub extern "system" fn Java_com_staticgraph_runtime_nativeaccess_NativeTopologyNatives_firstTouchMemory0(
    _env: JNIEnv,
    _class: JClass,
    address: JLong,
    bytes: JLong,
) -> JInt {
    platform::first_touch_memory(address, bytes)
}

#[allow(dead_code)]
fn cpu_list_contains(spec: &str, wanted_cpu: usize) -> bool {
    for segment in spec.trim().split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }

        let (range, selector) = if let Some((range, selector)) = segment.split_once(':') {
            let Some(selector) = parse_cpu_list_selector(selector.trim()) else {
                continue;
            };
            (range.trim(), selector)
        } else {
            (segment, CpuListSelector::Stride(1))
        };

        if let Some((start, end)) = range.split_once('-') {
            let Ok(start) = start.trim().parse::<usize>() else {
                continue;
            };
            let Ok(end) = end.trim().parse::<usize>() else {
                continue;
            };
            if start <= wanted_cpu
                && wanted_cpu <= end
                && cpu_list_selector_matches(&selector, wanted_cpu - start)
            {
                return true;
            }
            continue;
        }

        if matches!(selector, CpuListSelector::Stride(1))
            && range.parse::<usize>().is_ok_and(|cpu| cpu == wanted_cpu)
        {
            return true;
        }
    }

    false
}

fn parse_cpu_list_selector(selector: &str) -> Option<CpuListSelector> {
    if let Some((used, group)) = selector.split_once('/') {
        let used = used.trim().parse::<usize>().ok()?;
        let group = group.trim().parse::<usize>().ok()?;
        if used == 0 || group == 0 || used > group {
            return None;
        }
        return Some(CpuListSelector::Group { used, group });
    }

    let stride = selector.parse::<usize>().ok()?;
    if stride == 0 {
        None
    } else {
        Some(CpuListSelector::Stride(stride))
    }
}

fn cpu_list_selector_matches(selector: &CpuListSelector, offset: usize) -> bool {
    match selector {
        CpuListSelector::Stride(stride) => offset % stride == 0,
        CpuListSelector::Group { used, group } => offset % group < *used,
    }
}

#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
mod platform {
    use super::{
        cpu_list_contains, JInt, JLong, CAP_AFFINITY, CAP_CURRENT_CPU, CAP_FIRST_TOUCH, CAP_LINUX,
        CAP_LOCAL_MEM_POLICY, CAP_NUMA_QUERY, JNI_WORD_COUNT,
    };
    use core::ffi::{c_int, c_long, c_ulong};
    use core::ptr;
    use std::fs;

    const CPU_SETSIZE: usize = 1024;
    const CPU_WORD_BITS: usize = usize::BITS as usize;
    const CPU_WORD_COUNT: usize = CPU_SETSIZE / CPU_WORD_BITS;

    const EINVAL: i32 = 22;
    const ENODEV: i32 = 19;
    const ENOSYS: i32 = 38;
    const ERANGE: i32 = 34;

    const SC_NPROCESSORS_CONF: c_int = 83;
    const SC_NPROCESSORS_ONLN: c_int = 84;
    const MPOL_LOCAL: c_int = 4;

    #[cfg(target_arch = "x86_64")]
    const SYS_GETCPU: c_long = 309;
    #[cfg(target_arch = "aarch64")]
    const SYS_GETCPU: c_long = 168;

    #[cfg(target_arch = "x86_64")]
    const SYS_SET_MEMPOLICY: c_long = 238;
    #[cfg(target_arch = "aarch64")]
    const SYS_SET_MEMPOLICY: c_long = 237;

    #[repr(C)]
    struct CpuSet {
        bits: [usize; CPU_WORD_COUNT],
    }

    extern "C" {
        fn sched_setaffinity(pid: c_int, cpusetsize: usize, mask: *const CpuSet) -> c_int;
        fn sched_getcpu() -> c_int;
        fn sysconf(name: c_int) -> c_long;
        fn syscall(num: c_long, ...) -> c_long;
        fn __errno_location() -> *mut c_int;
        fn getpagesize() -> c_int;
    }

    pub fn native_capabilities() -> u64 {
        let mut caps =
            CAP_LINUX | CAP_AFFINITY | CAP_CURRENT_CPU | CAP_NUMA_QUERY | CAP_FIRST_TOUCH;

        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        {
            caps |= CAP_LOCAL_MEM_POLICY;
        }

        caps
    }

    pub fn max_cpu_count() -> JInt {
        CPU_SETSIZE as JInt
    }

    pub fn configured_cpu_count() -> JInt {
        sysconf_count(SC_NPROCESSORS_CONF)
    }

    pub fn online_cpu_count() -> JInt {
        sysconf_count(SC_NPROCESSORS_ONLN)
    }

    pub fn current_cpu() -> JInt {
        let cpu = unsafe { sched_getcpu() };
        if cpu >= 0 {
            cpu
        } else {
            -last_errno()
        }
    }

    pub fn current_numa_node() -> JInt {
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        {
            let mut cpu: u32 = 0;
            let mut node: u32 = 0;
            let rc = unsafe {
                syscall(
                    SYS_GETCPU,
                    &mut cpu as *mut u32,
                    &mut node as *mut u32,
                    ptr::null::<c_ulong>(),
                )
            };

            if rc == 0 {
                return node as JInt;
            }

            let errno = last_errno();
            if errno != ENOSYS {
                return -errno;
            }
        }

        let cpu = current_cpu();
        if cpu < 0 {
            cpu
        } else {
            numa_node_of_cpu(cpu)
        }
    }

    pub fn numa_node_of_cpu(cpu: JInt) -> JInt {
        if !(0..CPU_SETSIZE as JInt).contains(&cpu) {
            return -EINVAL;
        }

        let entries = match fs::read_dir("/sys/devices/system/node") {
            Ok(entries) => entries,
            Err(err) => return -err.raw_os_error().unwrap_or(ENODEV),
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(node_text) = name.strip_prefix("node") else {
                continue;
            };
            let Ok(node) = node_text.parse::<JInt>() else {
                continue;
            };

            let cpulist_path = entry.path().join("cpulist");
            let Ok(cpulist) = fs::read_to_string(cpulist_path) else {
                continue;
            };
            if cpu_list_contains(&cpulist, cpu as usize) {
                return node;
            }
        }

        -ENODEV
    }

    pub fn pin_current_thread_to_cpu(cpu: JInt) -> JInt {
        if !(0..CPU_SETSIZE as JInt).contains(&cpu) {
            return -EINVAL;
        }

        let mut set = CpuSet {
            bits: [0; CPU_WORD_COUNT],
        };
        let word = cpu as usize / CPU_WORD_BITS;
        let bit = cpu as usize % CPU_WORD_BITS;
        set.bits[word] |= 1usize << bit;

        set_affinity(&set)
    }

    pub fn pin_current_thread_to_cpu_mask(words: [JLong; JNI_WORD_COUNT]) -> JInt {
        if CPU_WORD_COUNT != JNI_WORD_COUNT {
            return -ERANGE;
        }

        let mut set = CpuSet {
            bits: [0; CPU_WORD_COUNT],
        };
        let mut non_empty = false;

        for (index, word) in words.into_iter().enumerate() {
            let bits = word as u64 as usize;
            set.bits[index] = bits;
            non_empty |= bits != 0;
        }

        if !non_empty {
            return -EINVAL;
        }

        set_affinity(&set)
    }

    pub fn set_local_allocation_policy() -> JInt {
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        {
            let rc = unsafe {
                syscall(
                    SYS_SET_MEMPOLICY,
                    MPOL_LOCAL,
                    ptr::null::<c_ulong>(),
                    0 as c_ulong,
                )
            };
            if rc == 0 {
                0
            } else {
                -last_errno()
            }
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            -ENOSYS
        }
    }

    pub fn first_touch_memory(address: JLong, bytes: JLong) -> JInt {
        if address <= 0 || bytes < 0 {
            return -EINVAL;
        }
        if bytes == 0 {
            return 0;
        }

        let Ok(start) = usize::try_from(address) else {
            return -EINVAL;
        };
        let Ok(length) = usize::try_from(bytes) else {
            return -EINVAL;
        };
        let Some(end_exclusive) = start.checked_add(length) else {
            return -EINVAL;
        };

        let page_size = unsafe { getpagesize() };
        if page_size <= 0 {
            return -last_errno();
        }
        let page_size = page_size as usize;
        let mut cursor = start;

        while cursor < end_exclusive {
            touch(cursor);

            let next_page = match next_page_boundary(cursor, page_size) {
                Some(next) => next,
                None => return -EINVAL,
            };
            if next_page <= cursor {
                return -EINVAL;
            }
            cursor = next_page;
        }

        0
    }

    fn sysconf_count(name: c_int) -> JInt {
        let count = unsafe { sysconf(name) };
        if count >= 0 {
            count as JInt
        } else {
            -last_errno()
        }
    }

    fn set_affinity(set: &CpuSet) -> JInt {
        let rc = unsafe { sched_setaffinity(0, core::mem::size_of::<CpuSet>(), set) };
        if rc == 0 {
            0
        } else {
            -last_errno()
        }
    }

    fn last_errno() -> JInt {
        unsafe { *__errno_location() }
    }

    fn next_page_boundary(address: usize, page_size: usize) -> Option<usize> {
        let page = address / page_size;
        page.checked_add(1)?.checked_mul(page_size)
    }

    fn touch(address: usize) {
        let pointer = address as *mut u8;
        unsafe {
            let value = pointer.read_volatile();
            pointer.write_volatile(value);
        }
    }
}

#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
mod platform {
    use super::{JInt, JLong, JNI_WORD_COUNT};

    const ENOSYS: JInt = 38;

    pub fn native_capabilities() -> u64 {
        0
    }

    pub fn max_cpu_count() -> JInt {
        -ENOSYS
    }

    pub fn configured_cpu_count() -> JInt {
        -ENOSYS
    }

    pub fn online_cpu_count() -> JInt {
        -ENOSYS
    }

    pub fn current_cpu() -> JInt {
        -ENOSYS
    }

    pub fn current_numa_node() -> JInt {
        -ENOSYS
    }

    pub fn numa_node_of_cpu(_cpu: JInt) -> JInt {
        -ENOSYS
    }

    pub fn pin_current_thread_to_cpu(_cpu: JInt) -> JInt {
        -ENOSYS
    }

    pub fn pin_current_thread_to_cpu_mask(_words: [JLong; JNI_WORD_COUNT]) -> JInt {
        -ENOSYS
    }

    pub fn set_local_allocation_policy() -> JInt {
        -ENOSYS
    }

    pub fn first_touch_memory(_address: JLong, _bytes: JLong) -> JInt {
        -ENOSYS
    }
}

#[cfg(test)]
mod tests {
    use super::cpu_list_contains;

    #[test]
    fn cpulist_single_values() {
        assert!(cpu_list_contains("0,2,4", 2));
        assert!(!cpu_list_contains("0,2,4", 3));
    }

    #[test]
    fn cpulist_ranges() {
        assert!(cpu_list_contains("0-3,8,10-12", 11));
        assert!(cpu_list_contains("0-3,8,10-12", 0));
        assert!(!cpu_list_contains("0-3,8,10-12", 9));
    }

    #[test]
    fn cpulist_stride_ranges() {
        assert!(cpu_list_contains("0-8:2,16", 6));
        assert!(cpu_list_contains("0-8:2,16", 16));
        assert!(!cpu_list_contains("0-8:2,16", 7));
        assert!(!cpu_list_contains("0-8:0,16", 8));
    }

    #[test]
    fn cpulist_grouped_ranges() {
        assert!(cpu_list_contains("0-15:2/4", 4));
        assert!(cpu_list_contains("0-15:2/4", 5));
        assert!(!cpu_list_contains("0-15:2/4", 6));
        assert!(!cpu_list_contains("0-15:5/4", 4));
    }

    #[test]
    fn cpulist_ignores_invalid_segments() {
        assert!(cpu_list_contains("bad,5-7", 6));
        assert!(!cpu_list_contains("bad,5-7", 8));
    }
}
