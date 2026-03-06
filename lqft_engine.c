#define PY_SSIZE_T_CLEAN
#include <Python.h>

#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS 
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#ifndef _MSC_VER
#include <unistd.h>
#endif

/**
 * LQFT C-Engine - v1.0.5 (Stabilization Build)
 * Architect: Parjad Minooei
 * * SYSTEMS ARCHITECTURE MILESTONES:
 * 1. F-04 RESOLUTION: 16k Hyper-Sharding eliminates lock contention to beat the 60s/10M budget.
 * 2. F-05 RESOLUTION: 2-Level Look-Ahead Prefetching masks DRAM latency during O(1) traversals.
 * 3. F-01/F-08 RESOLUTION: Eager Page Faulting via background daemon prevents access violations.
 * 4. F-02 RESOLUTION: Standardized metric sharding for native Python len() compatibility.
 */

#if defined(_MSC_VER)
    #include <windows.h>
    #include <memoryapi.h>
    #include <intrin.h>
    #pragma intrinsic(_InterlockedIncrement)
    #pragma intrinsic(_InterlockedDecrement)
    #pragma intrinsic(_InterlockedCompareExchange)
    #pragma intrinsic(_InterlockedExchange)
    #pragma intrinsic(_InterlockedCompareExchangePointer)
    #pragma intrinsic(_InterlockedExchangeAdd64)
    #define ATOMIC_INC(ptr) _InterlockedIncrement((LONG volatile*)(ptr))
    #define ATOMIC_DEC(ptr) _InterlockedDecrement((LONG volatile*)(ptr))
    #define PREFETCH(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
    #define CPU_PAUSE _mm_pause()
    #define ALIGN_64 __declspec(align(64))
    #define THREAD_LOCAL __declspec(thread)

    // OS-Native Reader-Writer Locks (Windows SRWLOCK)
    typedef SRWLOCK lqft_rwlock_t;
    #define LQFT_RWLOCK_INIT(lock) InitializeSRWLock(lock)
    #define LQFT_RWLOCK_RDLOCK(lock) AcquireSRWLockShared(lock)
    #define LQFT_RWLOCK_WRLOCK(lock) AcquireSRWLockExclusive(lock)
    #define LQFT_RWLOCK_UNLOCK_RD(lock) ReleaseSRWLockShared(lock)
    #define LQFT_RWLOCK_UNLOCK_WR(lock) ReleaseSRWLockExclusive(lock)
#else
    #include <pthread.h>
    #include <sched.h>
    #include <sys/mman.h>
    
    #ifndef MAP_POPULATE
    #define MAP_POPULATE 0
    #endif
    
    #define ATOMIC_INC(ptr) __sync_add_and_fetch((ptr), 1)
    #define ATOMIC_DEC(ptr) __sync_sub_and_fetch((ptr), 1)
    #define PREFETCH(ptr) __builtin_prefetch((const void*)(ptr), 0, 3)
    #define ALIGN_64 __attribute__((aligned(64)))
    #define THREAD_LOCAL __thread
    #if defined(__i386__) || defined(__x86_64__)
        #define CPU_PAUSE __asm__ volatile("pause\n": : :"memory")
    #elif defined(__aarch64__) || defined(__arm__)
        #define CPU_PAUSE __asm__ volatile("yield\n": : :"memory")
    #else
        #define CPU_PAUSE do {} while(0)
    #endif

    // OS-Native Reader-Writer Locks (POSIX pthread_rwlock)
    typedef pthread_rwlock_t lqft_rwlock_t;
    #define LQFT_RWLOCK_INIT(lock) pthread_rwlock_init(lock, NULL)
    #define LQFT_RWLOCK_RDLOCK(lock) pthread_rwlock_rdlock(lock)
    #define LQFT_RWLOCK_WRLOCK(lock) pthread_rwlock_wrlock(lock)
    #define LQFT_RWLOCK_UNLOCK_RD(lock) pthread_rwlock_unlock(lock)
    #define LQFT_RWLOCK_UNLOCK_WR(lock) pthread_rwlock_unlock(lock)
#endif

// ===================================================================
// CACHE-ALIGNED INFRASTRUCTURE & TTAS LOCKS
// ===================================================================

typedef struct {
    volatile long flag;
    char padding[60]; 
} ALIGN_64 PaddedLock;

typedef struct {
    lqft_rwlock_t lock;
    char padding[56]; 
} ALIGN_64 PaddedRWLock;

/**
 * Test-and-Test-and-Set (TTAS) Spinlock logic.
 * Minimizes cache-line invalidation traffic across the interconnect.
 */
static inline void fast_lock_backoff(volatile long* lk) {
    int spin = 0;
    for (;;) {
        if (*lk == 0) {
#ifdef _MSC_VER
            if (_InterlockedCompareExchange(lk, 1, 0) == 0) return;
#else
            if (__sync_val_compare_and_swap(lk, 0, 1) == 0) return;
#endif
        }
        spin++;
        int max_spin = 1 << (spin < 10 ? spin : 10);
        for(volatile int s = 0; s < max_spin; s++) { CPU_PAUSE; }
        if (spin > 1000) {
#ifdef _MSC_VER
            SwitchToThread();
#else
            sched_yield();
#endif
            spin = 0;
        }
    }
}

static inline void fast_unlock(volatile long* lk) {
#ifdef _MSC_VER
    _InterlockedExchange(lk, 0);
#else
    __sync_lock_release(lk);
#endif
}

// ===================================================================
// GLOBAL METRIC SHARDING (F-02)
// ===================================================================

#define MAX_TRACKED_THREADS 4096

typedef struct {
    int64_t phys_added;
    int64_t phys_freed;
    int64_t logical_inserts;
    int64_t child_bytes_added;
    int64_t child_bytes_freed;
    char padding[24]; 
} ALIGN_64 ThreadMetrics;

static ALIGN_64 ThreadMetrics global_metrics_array[MAX_TRACKED_THREADS];
static volatile long registered_threads_count = 0;
static THREAD_LOCAL ThreadMetrics* my_metrics = NULL;
static volatile long global_arena_epoch = 1;
static THREAD_LOCAL long local_arena_epoch = 0;

static inline ThreadMetrics* get_my_metrics() {
    if (my_metrics == NULL) {
#ifdef _MSC_VER
        long idx = _InterlockedIncrement(&registered_threads_count) - 1;
#else
        long idx = __sync_fetch_and_add(&registered_threads_count, 1);
#endif
        if (idx < MAX_TRACKED_THREADS) {
            my_metrics = &global_metrics_array[idx];
        } else {
            // Overflow bucket to avoid distorting thread-0 metrics.
            my_metrics = &global_metrics_array[MAX_TRACKED_THREADS - 1];
        }
    }
    return my_metrics;
}

#define BIT_PARTITION 5
#define MASK 0x1F 

// F-04: 16,384 Hyper-Shards for High-Throughput Write Availability
#define NUM_STRIPES 16384
#define STRIPE_SIZE 2048 
#define STRIPE_MASK (STRIPE_SIZE - 1)
#define TOMBSTONE ((LQFTNode*)1)

#define NUM_ROOTS 16384
#define ROOT_MASK 0x3FFF
#define ARENA_CHUNK_SIZE 16384

typedef struct LQFTNode {
    void* value;
    uint64_t key_hash;
    struct LQFTNode** children; 
    uint64_t full_hash_val;
    uint32_t value_len;
    int ref_count;
    uint32_t child_bitmap;
    uint16_t registry_idx;
    uint8_t child_count;
} LQFTNode;

typedef struct {
    PyObject* key_obj;
    PyObject* value_obj;
    const char* key_utf8;
    uint64_t hash;
    Py_ssize_t key_len;
    uint8_t fingerprint;
    uint8_t state;
} MutableEntry;

typedef struct {
    MutableEntry* table;
    size_t capacity;
    size_t size;
    size_t used;
    size_t tombstones;
} MutableTable;

typedef struct {
    PyObject_HEAD
    MutableTable table_state;
} NativeMutableLQFTObject;

static PyTypeObject NativeMutableLQFTType;

#define MUTABLE_EMPTY 0
#define MUTABLE_OCCUPIED 1
#define MUTABLE_DELETED 2

static void mutable_clear_all(MutableTable* table_state);
static PyObject* mutable_build_metrics(MutableTable* table_state);
static PyObject* mutable_export_items_from_table(MutableTable* table_state);
static MutableEntry* mutable_lookup_entry(MutableTable* table_state, const char* key, Py_ssize_t key_len);
static PyObject* method_mutable_new(PyObject* self, PyObject* args);
static PyObject* method_mutable_insert_key_value(PyObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* method_mutable_search_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* method_mutable_contains_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* method_mutable_delete_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* method_mutable_clear(PyObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* method_mutable_len(PyObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* method_mutable_get_metrics(PyObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* method_mutable_export_items(PyObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* native_mutable_new(PyTypeObject* type, PyObject* args, PyObject* kwds);
static int native_mutable_init(NativeMutableLQFTObject* self, PyObject* args, PyObject* kwds);
static void native_mutable_dealloc(NativeMutableLQFTObject* self);
static PyObject* native_mutable_insert(NativeMutableLQFTObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* native_mutable_search(NativeMutableLQFTObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* native_mutable_contains(NativeMutableLQFTObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* native_mutable_delete(NativeMutableLQFTObject* self, PyObject* const* args, Py_ssize_t nargs);
static PyObject* native_mutable_clear_method(NativeMutableLQFTObject* self, PyObject* args);
static PyObject* native_mutable_get_metrics_method(NativeMutableLQFTObject* self, PyObject* args);
static PyObject* native_mutable_export_items_method(NativeMutableLQFTObject* self, PyObject* args);
static Py_ssize_t native_mutable_len(NativeMutableLQFTObject* self);

// ===================================================================
// THREAD-LOCAL ARENA ALLOCATOR (F-01/F-08 Stabilization)
// ===================================================================

typedef struct NodeChunk {
    LQFTNode nodes[ARENA_CHUNK_SIZE];
    struct NodeChunk* next_global; 
} NodeChunk;

static inline NodeChunk* alloc_node_chunk(void) {
#ifdef _MSC_VER
    return (NodeChunk*)VirtualAlloc(NULL, sizeof(NodeChunk), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
#else
    void* p = mmap(NULL, sizeof(NodeChunk), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    return (p == MAP_FAILED) ? NULL : (NodeChunk*)p;
#endif
}

static inline void free_node_chunk(NodeChunk* chunk) {
    if (!chunk) return;
#ifdef _MSC_VER
    VirtualFree(chunk, 0, MEM_RELEASE);
#else
    munmap(chunk, sizeof(NodeChunk));
#endif
}

static PaddedLock global_chunk_lock = {0};
static NodeChunk* global_node_chunks = NULL;

static NodeChunk* volatile pre_zeroed_node_chunks = NULL;
static volatile long pre_node_count = 0;
static volatile long bg_alloc_running = 0;

typedef struct {
    LQFTNode* volatile head;
    char padding[56];
} ALIGN_64 GlobalNodePool;

static GlobalNodePool node_pool = {NULL};

typedef struct {
    NodeChunk* current_node_chunk;
    int node_chunk_idx;
    LQFTNode* node_free_list;
} TLS_Arena;

static THREAD_LOCAL TLS_Arena local_arena = {NULL, ARENA_CHUNK_SIZE, NULL};

// Batched GC Retirement Chains
static THREAD_LOCAL LQFTNode* local_ret_node_head = NULL;
static THREAD_LOCAL LQFTNode* local_ret_node_tail = NULL;
static THREAD_LOCAL int local_ret_node_count = 0;

static inline void reset_tls_state_if_needed(void) {
    long ge = global_arena_epoch;
    if (local_arena_epoch == ge) return;

    // A global purge/free_all occurred. Drop stale per-thread pointers.
    local_arena.current_node_chunk = NULL;
    local_arena.node_chunk_idx = ARENA_CHUNK_SIZE;
    local_arena.node_free_list = NULL;

    local_ret_node_head = NULL;
    local_ret_node_tail = NULL;
    local_ret_node_count = 0;

    local_arena_epoch = ge;
}

static inline NodeChunk* pop_pre_zeroed_node_chunk(void) {
    NodeChunk* head;
    for (;;) {
        head = pre_zeroed_node_chunks;
        if (!head) return NULL;
#ifdef _MSC_VER
        if (_InterlockedCompareExchangePointer((void* volatile*)&pre_zeroed_node_chunks, (void*)head->next_global, (void*)head) == (void*)head) {
            _InterlockedDecrement(&pre_node_count);
            return head;
        }
#else
        if (__sync_bool_compare_and_swap(&pre_zeroed_node_chunks, head, head->next_global)) {
            __sync_fetch_and_sub(&pre_node_count, 1);
            return head;
        }
#endif
    }
}

static LQFTNode** registry = NULL;

typedef struct {
    LQFTNode* root;
    char padding[56];
} ALIGN_64 PaddedRoot;

static ALIGN_64 PaddedRoot global_roots[NUM_ROOTS];

static ALIGN_64 PaddedRWLock root_locks[NUM_ROOTS];
static ALIGN_64 PaddedLock stripe_locks[NUM_STRIPES];
static volatile long global_reads_sealed = 0;

#define VALUE_POOL_BUCKETS 4096

typedef struct ValueEntry {
    char* str;
    uint64_t hash;
    uint32_t len;
    volatile long ref_count;
    struct ValueEntry* next;
} ValueEntry;

static ValueEntry* value_pool[VALUE_POOL_BUCKETS] = {0};
static ALIGN_64 PaddedLock value_pool_locks[VALUE_POOL_BUCKETS];
static int64_t value_pool_entry_count = 0;
static int64_t value_pool_total_bytes = 0;

static const char* value_acquire(const char* value_ptr, uint64_t value_hash, uint32_t value_len);
static void value_release(const char* value_ptr, uint64_t value_hash);
static void value_pool_clear_all(void);

const uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
const uint64_t FNV_PRIME = 1099511628211ULL;

static inline uint64_t fnv1a_update(uint64_t hash, const void* data, size_t len) {
    const uint8_t* p = (const uint8_t*)data;
    for (size_t i = 0; i < len; i++) {
        hash ^= p[i];
        hash *= FNV_PRIME;
    }
    return hash;
}

static inline uint64_t hash_bytes_64(const char* data, uint32_t len) {
    return fnv1a_update(FNV_OFFSET_BASIS, data, len);
}

static inline uint64_t hash_node_state(LQFTNode** children) {
    uint64_t hval = 0;
    if (children) {
        for (int i = 0; i < 32; i++) {
            if (children[i]) {
                hval ^= (children[i]->full_hash_val + i);
            }
        }
    }
    return (hval * FNV_PRIME) ^ FNV_OFFSET_BASIS;
}

static inline uint32_t popcount32(uint32_t value) {
#if defined(_MSC_VER)
    return (uint32_t)__popcnt(value);
#else
    return (uint32_t)__builtin_popcount(value);
#endif
}

static inline LQFTNode* node_get_child(const LQFTNode* node, uint32_t segment) {
    if (!node || !node->children) return NULL;
    uint32_t bit = (uint32_t)1u << segment;
    uint32_t bitmap = node->child_bitmap;
    if ((bitmap & bit) == 0) return NULL;
    uint32_t idx = popcount32(bitmap & (bit - 1u));
    return node->children[idx];
}

static inline void node_expand_children(const LQFTNode* node, LQFTNode** out_children) {
    memset(out_children, 0, sizeof(LQFTNode*) * 32);
    if (!node || !node->children) return;
    uint32_t bitmap = node->child_bitmap;
    uint32_t idx = 0;
    while (bitmap) {
        uint32_t segment = popcount32((bitmap & (uint32_t)(-(int32_t)bitmap)) - 1u);
        out_children[segment] = node->children[idx++];
        bitmap &= (bitmap - 1u);
    }
}

char* portable_strdup(const char* s) {
    if (!s) return NULL;
#ifdef _WIN32
    return _strdup(s);
#else
    return strdup(s);
#endif
}

/**
 * F-01 & F-08: Background Allocation Daemon.
 * Uses memset to force the OS to physically map pages into RAM before the workers start.
 */
#ifdef _MSC_VER
DWORD WINAPI background_alloc_thread(LPVOID arg) {
#else
void* background_alloc_thread(void* arg) {
#endif
    while(bg_alloc_running) {
        int work_done = 0;
        if (pre_node_count < 128) {
            NodeChunk* nc = alloc_node_chunk();
            if (nc) {
                memset(nc, 0, sizeof(NodeChunk)); // Eager Page Fault
#ifdef _MSC_VER
                NodeChunk* old;
                do { old = pre_zeroed_node_chunks; nc->next_global = old; } 
                while (_InterlockedCompareExchangePointer((void* volatile*)&pre_zeroed_node_chunks, (void*)nc, (void*)old) != (void*)old);
                _InterlockedIncrement(&pre_node_count);
#else
                NodeChunk* old;
                do { old = pre_zeroed_node_chunks; nc->next_global = old; } 
                while (!__sync_bool_compare_and_swap(&pre_zeroed_node_chunks, old, nc));
                __sync_fetch_and_add(&pre_node_count, 1);
#endif
                work_done = 1;
            }
        }
        if (!work_done) {
#ifdef _MSC_VER
            Sleep(1);
#else
            usleep(1000);
#endif
        }
    }
    return 0;
}

LQFTNode* create_node(void* value, uint32_t value_len, uint64_t key_hash, LQFTNode** children_src, uint64_t full_hash) {
    reset_tls_state_if_needed();

    LQFTNode* node = NULL;
    if (!local_arena.node_free_list) {
#ifdef _MSC_VER
        LQFTNode* free_chain;
        do {
            free_chain = (LQFTNode*)node_pool.head;
            if (!free_chain) break;
        } while (_InterlockedCompareExchangePointer((void* volatile*)&node_pool.head, NULL, (void*)free_chain) != (void*)free_chain);
        local_arena.node_free_list = free_chain;
#else
        LQFTNode* free_chain;
        do {
            free_chain = (LQFTNode*)node_pool.head;
            if (!free_chain) break;
        } while (!__sync_bool_compare_and_swap(&node_pool.head, free_chain, NULL));
        local_arena.node_free_list = free_chain;
#endif
    }
    if (local_arena.node_free_list) {
        node = local_arena.node_free_list;
        local_arena.node_free_list = (LQFTNode*)node->children;
    } else {
        if (local_arena.node_chunk_idx >= ARENA_CHUNK_SIZE) {
            NodeChunk* new_chunk = pop_pre_zeroed_node_chunk();
            if (!new_chunk) {
                new_chunk = alloc_node_chunk();
            }
            if (!new_chunk) return NULL;
            local_arena.current_node_chunk = new_chunk;
            local_arena.node_chunk_idx = 0;
            fast_lock_backoff(&global_chunk_lock.flag);
            if (new_chunk) new_chunk->next_global = global_node_chunks;
            global_node_chunks = new_chunk;
            fast_unlock(&global_chunk_lock.flag);
        }
        node = &local_arena.current_node_chunk->nodes[local_arena.node_chunk_idx++];
    }
    node->value = value;
    node->value_len = value_len;
    node->key_hash = key_hash;
    node->full_hash_val = full_hash; 
    node->registry_idx = 0;
    node->ref_count = 0;
    node->child_bitmap = 0;
    node->child_count = 0;
    if (children_src) {
        uint32_t bitmap = 0;
        uint8_t child_count = 0;
        for (uint32_t i = 0; i < 32; i++) {
            if (children_src[i]) {
                bitmap |= ((uint32_t)1u << i);
                child_count++;
            }
        }
        node->child_bitmap = bitmap;
        node->child_count = child_count;
        if (child_count > 0) {
            size_t child_bytes = (size_t)child_count * sizeof(LQFTNode*);
            LQFTNode** arr = (LQFTNode**)malloc(child_bytes);
            if (!arr) {
                node->children = (LQFTNode**)local_arena.node_free_list;
                local_arena.node_free_list = node;
                return NULL;
            }
            uint32_t out_idx = 0;
            for (uint32_t i = 0; i < 32; i++) {
                if (children_src[i]) arr[out_idx++] = children_src[i];
            }
            node->children = arr;
            get_my_metrics()->child_bytes_added += (int64_t)child_bytes;
        } else {
            node->children = NULL;
        }
    } else {
        node->children = NULL; 
    }
    return node;
}

void decref(LQFTNode* start_node) {
    if (!start_node || start_node == TOMBSTONE) return;
    int cap = 64;
    int top = 0;
    LQFTNode* local_stack[64];
    LQFTNode** cleanup_stack = local_stack;
    cleanup_stack[top++] = start_node;
    while (top > 0) {
        LQFTNode* node = cleanup_stack[--top];
        int new_ref = ATOMIC_DEC(&node->ref_count);
        if (new_ref == 0) {
            uint32_t stripe = (uint32_t)(node->full_hash_val % NUM_STRIPES);
            uint32_t global_idx = (stripe * STRIPE_SIZE) + node->registry_idx;
            fast_lock_backoff(&stripe_locks[stripe].flag);
            if (registry[global_idx] == node) registry[global_idx] = TOMBSTONE;
            fast_unlock(&stripe_locks[stripe].flag);
            if (node->children) {
                for (uint8_t i = 0; i < node->child_count; i++) {
                    if (top >= cap) {
                        int next_cap = cap * 2;
                        LQFTNode** grown;
                        if (cleanup_stack == local_stack) {
                            grown = (LQFTNode**)malloc((size_t)next_cap * sizeof(LQFTNode*));
                            if (grown) memcpy(grown, local_stack, (size_t)top * sizeof(LQFTNode*));
                        } else {
                            grown = (LQFTNode**)realloc(cleanup_stack, (size_t)next_cap * sizeof(LQFTNode*));
                        }
                        if (!grown) {
                            if (cleanup_stack != local_stack) free(cleanup_stack);
                            return;
                        }
                        cleanup_stack = grown;
                        cap = next_cap;
                    }
                    cleanup_stack[top++] = node->children[i];
                }
                get_my_metrics()->child_bytes_freed += (int64_t)node->child_count * (int64_t)sizeof(LQFTNode*);
                free(node->children);
            }
            if (node->value) value_release((const char*)node->value, hash_bytes_64((const char*)node->value, node->value_len));
            node->children = (LQFTNode**)local_ret_node_head;
            local_ret_node_head = node;
            if (local_ret_node_count == 0) local_ret_node_tail = node;
            local_ret_node_count++;
            if (local_ret_node_count >= 1024) {
#ifdef _MSC_VER
                LQFTNode* old_node_head;
                do {
                    old_node_head = (LQFTNode*)node_pool.head;
                    local_ret_node_tail->children = (LQFTNode**)old_node_head;
                } while (_InterlockedCompareExchangePointer((void* volatile*)&node_pool.head, (void*)local_ret_node_head, (void*)old_node_head) != (void*)old_node_head);
#else
                LQFTNode* old_node_head;
                do {
                    old_node_head = (LQFTNode*)node_pool.head;
                    local_ret_node_tail->children = (LQFTNode**)old_node_head;
                } while (!__sync_bool_compare_and_swap(&node_pool.head, old_node_head, local_ret_node_head));
#endif
                local_ret_node_head = NULL;
                local_ret_node_tail = NULL;
                local_ret_node_count = 0;
            }
            get_my_metrics()->phys_freed++;
        }
    }
    if (cleanup_stack != local_stack) free(cleanup_stack);
}

static inline int node_matches_signature(const LQFTNode* node, const char* value_ptr, uint32_t value_len, uint64_t key_hash, LQFTNode** children) {
    if (!node) return 0;
    if (node->key_hash != key_hash) return 0;
    if (node->value_len != value_len) return 0;

    if ((node->value == NULL) != (value_ptr == NULL)) return 0;
    if (node->value && value_ptr && node->value != (const void*)value_ptr && memcmp((const char*)node->value, value_ptr, (size_t)value_len) != 0) return 0;

    uint32_t child_bitmap = 0;
    uint8_t child_count = 0;
    if (children) {
        for (int i = 0; i < 32; i++) {
            if (children[i]) {
                child_bitmap |= ((uint32_t)1u << i);
                child_count++;
            }
        }
    }
    if (node->child_bitmap != child_bitmap) return 0;
    if (node->child_count != child_count) return 0;
    if (node->children) {
        uint32_t idx = 0;
        for (int i = 0; i < 32; i++) {
            if (children[i] && node->children[idx++] != children[i]) return 0;
        }
    }
    return 1;
}

LQFTNode* get_canonical_v2(const char* value_ptr, uint64_t value_hash, uint32_t value_len, uint64_t key_hash, LQFTNode** children, uint64_t full_hash) {
    uint32_t stripe = (uint32_t)(full_hash % NUM_STRIPES);
    uint32_t local_idx = (uint32_t)((full_hash ^ (full_hash >> 32)) & STRIPE_MASK);
    uint32_t global_idx = (stripe * STRIPE_SIZE) + local_idx;
    uint32_t start_idx = local_idx;

    // Minimal stability hardening: protect canonical-registry probing with stripe lock
    // to avoid racing against concurrent tombstoning/reclamation.
    fast_lock_backoff(&stripe_locks[stripe].flag);
    for (;;) {
        LQFTNode* slot = registry[global_idx];
        if (slot == NULL) break;
        if (slot != TOMBSTONE && slot->full_hash_val == full_hash && node_matches_signature(slot, value_ptr, value_len, key_hash, children)) {
            ATOMIC_INC(&slot->ref_count);
            fast_unlock(&stripe_locks[stripe].flag);
            return slot;
        }
        local_idx = (local_idx + 1) & STRIPE_MASK;
        global_idx = (stripe * STRIPE_SIZE) + local_idx;
        if (local_idx == start_idx) break;
    }
    fast_unlock(&stripe_locks[stripe].flag);

    const char* canonical_value = value_ptr ? value_acquire(value_ptr, value_hash, value_len) : NULL;
    if (value_ptr && !canonical_value) return NULL;
    LQFTNode* new_node = create_node((void*)canonical_value, value_len, key_hash, children, full_hash);
    if (!new_node) return NULL;
    new_node->ref_count = 1;

    fast_lock_backoff(&stripe_locks[stripe].flag);
    local_idx = (uint32_t)((full_hash ^ (full_hash >> 32)) & STRIPE_MASK);
    global_idx = (stripe * STRIPE_SIZE) + local_idx;
    start_idx = local_idx;
    int first_tombstone = -1;
    for (;;) {
        LQFTNode* slot = registry[global_idx];
        if (slot == NULL) break;
        if (slot == TOMBSTONE) { if (first_tombstone == -1) first_tombstone = (int)local_idx; }
        else if (slot->full_hash_val == full_hash && node_matches_signature(slot, value_ptr, value_len, key_hash, children)) {
            ATOMIC_INC(&slot->ref_count);
            fast_unlock(&stripe_locks[stripe].flag);
            if (new_node->value) value_release((const char*)new_node->value, value_hash);
            if (new_node->children) {
                get_my_metrics()->child_bytes_freed += (int64_t)new_node->child_count * (int64_t)sizeof(LQFTNode*);
                free(new_node->children);
            }
            new_node->children = (LQFTNode**)local_ret_node_head;
            local_ret_node_head = new_node;
            if (local_ret_node_count == 0) local_ret_node_tail = new_node;
            local_ret_node_count++;
            return slot;
        }
        local_idx = (local_idx + 1) & STRIPE_MASK;
        global_idx = (stripe * STRIPE_SIZE) + local_idx;
        if (local_idx == start_idx) break; 
    }
    if (new_node->children) {
        for (uint8_t i = 0; i < new_node->child_count; i++) {
            ATOMIC_INC(&new_node->children[i]->ref_count);
        }
    }
    uint32_t insert_local = (first_tombstone != -1) ? (uint32_t)first_tombstone : local_idx;
    uint32_t insert_global = (stripe * STRIPE_SIZE) + insert_local;
    if (insert_local == start_idx && registry[insert_global] != NULL && registry[insert_global] != TOMBSTONE) {
        fast_unlock(&stripe_locks[stripe].flag);
        return new_node;
    }
    new_node->registry_idx = insert_local; 
    registry[insert_global] = new_node;
    get_my_metrics()->phys_added++;
    fast_unlock(&stripe_locks[stripe].flag);
    return new_node;
}

LQFTNode* core_insert_internal(uint64_t h, const char* val_ptr, uint64_t val_hash, uint32_t val_len, LQFTNode* root, uint64_t pre_leaf_base) {
    LQFTNode* path_nodes[20]; uint32_t path_segs[20]; int path_len = 0;
    LQFTNode* curr = root; int bit_depth = 0;
    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        path_nodes[path_len] = curr; path_segs[path_len] = segment; path_len++;
        LQFTNode* next_node = node_get_child(curr, segment);
        if (next_node == NULL) { curr = NULL; break; }
        PREFETCH(next_node);
        curr = next_node; 
        bit_depth += BIT_PARTITION;
    }
    LQFTNode* new_sub_node = NULL;
    uint64_t leaf_h = (pre_leaf_base ^ h) * FNV_PRIME;
    if (curr == NULL) {
        new_sub_node = get_canonical_v2(val_ptr, val_hash, val_len, h, NULL, leaf_h);
    } else if (curr->key_hash == h) {
        LQFTNode* curr_children[32];
        node_expand_children(curr, curr_children);
        new_sub_node = get_canonical_v2(val_ptr, val_hash, val_len, h, curr_children, leaf_h);
    } else {
        uint64_t old_h = curr->key_hash;
        uint64_t old_leaf_h = (pre_leaf_base ^ old_h) * FNV_PRIME;
        int temp_depth = bit_depth;
        while (temp_depth < 64) {
            uint32_t s_old = (old_h >> temp_depth) & MASK;
            uint32_t s_new = (h >> temp_depth) & MASK;
            if (s_old != s_new) {
                uint64_t old_value_hash = curr->value ? hash_bytes_64((const char*)curr->value, curr->value_len) : 0;
                LQFTNode* curr_children[32];
                node_expand_children(curr, curr_children);
                LQFTNode* c_old = get_canonical_v2((const char*)curr->value, old_value_hash, curr->value_len, old_h, curr_children, old_leaf_h);
                LQFTNode* c_new = get_canonical_v2(val_ptr, val_hash, val_len, h, NULL, leaf_h);
                LQFTNode* new_children[32]; memset(new_children, 0, sizeof(LQFTNode*) * 32);
                new_children[s_old] = c_old; new_children[s_new] = c_new;
                uint64_t branch_h = hash_node_state(new_children);
                new_sub_node = get_canonical_v2(NULL, 0, 0, 0, new_children, branch_h);
                decref(c_old); decref(c_new); break;
            } else { 
                path_nodes[path_len] = NULL; path_segs[path_len] = s_old; path_len++; temp_depth += BIT_PARTITION; 
            }
        }
        if (new_sub_node == NULL) {
            LQFTNode* curr_children[32];
            node_expand_children(curr, curr_children);
            new_sub_node = get_canonical_v2(val_ptr, val_hash, val_len, h, curr_children, leaf_h);
        }
    }
    for (int i = path_len - 1; i >= 0; i--) {
        LQFTNode* next_parent;
        if (path_nodes[i] == NULL) {
            LQFTNode* new_children[32]; memset(new_children, 0, sizeof(LQFTNode*) * 32);
            new_children[path_segs[i]] = new_sub_node;
            next_parent = get_canonical_v2(NULL, 0, 0, 0, new_children, hash_node_state(new_children));
        } else {
            LQFTNode* p = path_nodes[i];
            LQFTNode* n_children[32]; 
            node_expand_children(p, n_children);
            n_children[path_segs[i]] = new_sub_node;
            uint64_t b_h = hash_node_state(n_children);
            uint64_t parent_value_hash = p->value ? hash_bytes_64((const char*)p->value, p->value_len) : 0;
            next_parent = get_canonical_v2((const char*)p->value, parent_value_hash, p->value_len, p->key_hash, n_children, b_h);
        }
        decref(new_sub_node); new_sub_node = next_parent;
    }
    return new_sub_node;
}

LQFTNode* core_delete_internal(uint64_t h, LQFTNode* root) {
    if (root == NULL) return NULL;
    LQFTNode* path_nodes[20]; uint32_t path_segs[20]; int path_len = 0;
    LQFTNode* curr = root; int bit_depth = 0;
    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        path_nodes[path_len] = curr; path_segs[path_len] = segment; path_len++;
        if (node_get_child(curr, segment) == NULL) {
            ATOMIC_INC(&root->ref_count);
            return root; 
        }
        curr = node_get_child(curr, segment); bit_depth += BIT_PARTITION;
    }
    if (curr == NULL || curr->key_hash != h) {
        ATOMIC_INC(&root->ref_count);
        return root; 
    }
    LQFTNode* new_sub_node = NULL; 
    for (int i = path_len - 1; i >= 0; i--) {
        LQFTNode* p = path_nodes[i];
        LQFTNode* n_children[32]; 
        node_expand_children(p, n_children);
        n_children[path_segs[i]] = new_sub_node;
        int has_c = 0; for(int j=0; j<32; j++) { if(n_children[j]) { has_c = 1; break; } }
        if (!has_c && p->value == NULL) { 
            new_sub_node = NULL; 
        } else {
            uint64_t b_h = hash_node_state(n_children);
            uint64_t parent_value_hash = p->value ? hash_bytes_64((const char*)p->value, p->value_len) : 0;
            new_sub_node = get_canonical_v2((const char*)p->value, parent_value_hash, p->value_len, p->key_hash, n_children, b_h);
        }
    }
    return new_sub_node;
}

char* core_search(uint64_t h, LQFTNode* root) {
    LQFTNode* curr = root; 
    int bit_depth = 0;
    while (curr != NULL && curr->value == NULL) {
        curr = node_get_child(curr, (h >> bit_depth) & MASK);
        bit_depth += BIT_PARTITION;
    }
    if (curr != NULL && curr->key_hash == h) return (char*)curr->value;
    return NULL;
}

static inline uint64_t hash_key_string(const char* key_str) {
    // One-pass FNV-1a for NUL-terminated UTF-8 keys (avoids strlen + second scan).
    uint64_t h = FNV_OFFSET_BASIS;
    const unsigned char* p = (const unsigned char*)key_str;
    while (*p) {
        h ^= (uint64_t)(*p++);
        h *= FNV_PRIME;
    }
    return h;
}

static inline uint64_t hash_key_bytes(const char* key_str, Py_ssize_t key_len) {
    uint64_t h = FNV_OFFSET_BASIS;
    const unsigned char* p = (const unsigned char*)key_str;
    for (Py_ssize_t i = 0; i < key_len; i++) {
        h ^= (uint64_t)p[i];
        h *= FNV_PRIME;
    }
    return h;
}

static THREAD_LOCAL PyObject* tls_last_key_obj = NULL;
static THREAD_LOCAL uint64_t tls_last_key_hash = 0;

static inline uint64_t hash_key_unicode_cached(PyObject* key_obj, const char* key_str, Py_ssize_t key_len) {
    if (key_obj == tls_last_key_obj) {
        return tls_last_key_hash;
    }
    uint64_t h = hash_key_bytes(key_str, key_len);
    Py_INCREF(key_obj);
    Py_XDECREF(tls_last_key_obj);
    tls_last_key_obj = key_obj;
    tls_last_key_hash = h;
    return h;
}

static inline uint64_t fnv1a_update_u64_decimal(uint64_t hash, uint64_t value) {
    // Append unsigned integer digits directly to FNV stream without heap allocation.
    char rev[20];
    int len = 0;
    do {
        rev[len++] = (char)('0' + (value % 10));
        value /= 10;
    } while (value != 0);

    for (int i = len - 1; i >= 0; i--) {
        hash ^= (uint64_t)(unsigned char)rev[i];
        hash *= FNV_PRIME;
    }
    return hash;
}

static inline uint64_t build_leaf_prefix_hash(const char* val_str, uint32_t val_len) {
    uint64_t pre = fnv1a_update(FNV_OFFSET_BASIS, "leaf:", 5);
    return fnv1a_update(pre, val_str, val_len);
}

static void c_internal_insert_rw_precomputed(uint64_t h, const char* val_str, uint64_t pre, uint64_t val_hash, uint32_t val_len) {
    ThreadMetrics* metrics = get_my_metrics();
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    metrics->logical_inserts++;
    while (1) {
        LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
        LQFTNode* old_root = global_roots[shard].root;
        if (old_root) ATOMIC_INC(&old_root->ref_count);
        LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
        LQFTNode* next = core_insert_internal(h, val_str, val_hash, val_len, old_root, pre);
        LQFT_RWLOCK_WRLOCK(&root_locks[shard].lock);
        if (global_roots[shard].root == old_root) {
            global_roots[shard].root = next;
            LQFT_RWLOCK_UNLOCK_WR(&root_locks[shard].lock);
            if (old_root) { decref(old_root); decref(old_root); }
            break;
        } else {
            LQFT_RWLOCK_UNLOCK_WR(&root_locks[shard].lock);
            if (next) decref(next);
            if (old_root) decref(old_root);
            for(volatile int s = 0; s < 16; s++) { CPU_PAUSE; }
        }
    }
}

static void c_internal_insert_rw(uint64_t h, const char* val_str) {
    // Small TLS cache avoids repeated value hashing for hot constants (e.g. "x", "active").
    static THREAD_LOCAL const char* last_val_ptr = NULL;
    static THREAD_LOCAL uint64_t last_pre = 0;
    static THREAD_LOCAL uint64_t last_val_hash = 0;
    static THREAD_LOCAL uint32_t last_val_len = 0;
    uint64_t pre;
    uint64_t val_hash;
    uint32_t val_len;
    if (val_str == last_val_ptr) {
        pre = last_pre;
        val_hash = last_val_hash;
        val_len = last_val_len;
    } else {
        size_t val_size = strlen(val_str);
        pre = fnv1a_update(FNV_OFFSET_BASIS, "leaf:", 5);
        pre = fnv1a_update(pre, val_str, val_size);
        val_hash = fnv1a_update(FNV_OFFSET_BASIS, val_str, val_size);
        val_len = (uint32_t)val_size;
        last_val_ptr = val_str;
        last_pre = pre;
        last_val_hash = val_hash;
        last_val_len = val_len;
    }
    c_internal_insert_rw_precomputed(h, val_str, pre, val_hash, val_len);
}

typedef struct {
    int thread_id;
    int ops;
    int write_threshold; 
} StressArgs;

static inline uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state;
    x ^= x << 13; x ^= x >> 17; x ^= x << 5;
    return *state = x;
}

#ifdef _MSC_VER
DWORD WINAPI stress_worker(LPVOID arg) {
#else
void* stress_worker(void* arg) {
#endif
    StressArgs* sargs = (StressArgs*)arg;
#ifdef _MSC_VER
    SetThreadAffinityMask(GetCurrentThread(), (DWORD_PTR)1 << (sargs->thread_id % 64));
#elif defined(__linux__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    long num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (num_cores <= 0) num_cores = 16;
    CPU_SET(sargs->thread_id % num_cores, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#endif
    local_arena.current_node_chunk = NULL;
    local_arena.node_chunk_idx = ARENA_CHUNK_SIZE;
    local_arena.node_free_list = NULL;
    local_ret_node_head = NULL;
    local_ret_node_tail = NULL;
    local_ret_node_count = 0;
    my_metrics = NULL;
    get_my_metrics();
    uint32_t rng_state = 123456789 ^ (sargs->thread_id * 1999999973);
    char val_buf[32] = "val";
    for (int i = 0; i < sargs->ops; i++) {
        uint32_t roll = xorshift32(&rng_state) % 100;
        uint64_t h = ((uint64_t)xorshift32(&rng_state) << 32) | xorshift32(&rng_state);
        if (roll < (uint32_t)sargs->write_threshold) {
            c_internal_insert_rw(h, val_buf);
        } else {
            uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
            LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
            LQFTNode* current_root = global_roots[shard].root;
            if (current_root) core_search(h, current_root);
            LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
        }
    }
    return 0;
}

static PyObject* method_internal_stress_test(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 3) return NULL;
    int ops = (int)PyLong_AsLong(args[0]);
    int num_threads = (int)PyLong_AsLong(args[1]);
    double write_ratio = PyFloat_AsDouble(args[2]);
    int write_threshold = (int)(write_ratio * 100.0);
    int ops_per_thread = ops / num_threads;
    StressArgs* t_args = (StressArgs*)malloc(sizeof(StressArgs) * num_threads);
    Py_BEGIN_ALLOW_THREADS
#ifdef _MSC_VER
    HANDLE* threads = (HANDLE*)malloc(sizeof(HANDLE) * num_threads);
    for (int i = 0; i < num_threads; i++) {
        t_args[i].thread_id = i;
        t_args[i].ops = ops_per_thread;
        t_args[i].write_threshold = write_threshold;
        threads[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)stress_worker, &t_args[i], 0, NULL);
    }
    WaitForMultipleObjects(num_threads, threads, TRUE, INFINITE);
    for (int i = 0; i < num_threads; i++) CloseHandle(threads[i]);
    free(threads);
#else
    pthread_t* threads = (pthread_t*)malloc(sizeof(pthread_t) * num_threads);
    for (int i = 0; i < num_threads; i++) {
        t_args[i].thread_id = i;
        t_args[i].ops = ops_per_thread;
        t_args[i].write_threshold = write_threshold;
        pthread_create(&threads[i], NULL, stress_worker, &t_args[i]);
    }
    for (int i = 0; i < num_threads; i++) pthread_join(threads[i], NULL);
    free(threads);
#endif
    Py_END_ALLOW_THREADS
    free(t_args);
    Py_RETURN_NONE;
}

static PyObject* method_set_reads_sealed(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    int enabled = PyObject_IsTrue(args[0]);
    if (enabled < 0) return NULL;
#ifdef _MSC_VER
    _InterlockedExchange(&global_reads_sealed, enabled ? 1 : 0);
#else
    __sync_lock_test_and_set(&global_reads_sealed, enabled ? 1 : 0);
#endif
    Py_RETURN_NONE;
}

static PyObject* method_insert(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    if (global_reads_sealed) {
        PyErr_SetString(PyExc_RuntimeError, "LQFT reads are sealed for lock-free mode; unseal before writing");
        return NULL;
    }
    uint64_t h = PyLong_AsUnsignedLongLongMask(args[0]);
    Py_ssize_t val_len_ssize = 0;
    const char* val_str = PyUnicode_AsUTF8AndSize(args[1], &val_len_ssize);
    if (!val_str) return NULL;
    uint32_t val_len = (uint32_t)val_len_ssize;
    uint64_t val_hash = hash_bytes_64(val_str, val_len);
    uint64_t pre = build_leaf_prefix_hash(val_str, val_len);
    Py_BEGIN_ALLOW_THREADS
    c_internal_insert_rw_precomputed(h, val_str, pre, val_hash, val_len);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* method_insert_key_value(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    if (global_reads_sealed) {
        PyErr_SetString(PyExc_RuntimeError, "LQFT reads are sealed for lock-free mode; unseal before writing");
        return NULL;
    }
    Py_ssize_t key_len = 0;
    const char* key_str = PyUnicode_AsUTF8AndSize(args[0], &key_len);
    Py_ssize_t val_len_ssize = 0;
    const char* val_str = PyUnicode_AsUTF8AndSize(args[1], &val_len_ssize);
    if (!key_str || !val_str) return NULL;
    uint64_t h = hash_key_unicode_cached(args[0], key_str, key_len);
    uint32_t val_len = (uint32_t)val_len_ssize;
    uint64_t val_hash = hash_bytes_64(val_str, val_len);
    uint64_t pre = build_leaf_prefix_hash(val_str, val_len);
    Py_BEGIN_ALLOW_THREADS
    c_internal_insert_rw_precomputed(h, val_str, pre, val_hash, val_len);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static THREAD_LOCAL uint64_t* tls_bulk_hashes = NULL;
static THREAD_LOCAL Py_ssize_t tls_bulk_hashes_cap = 0;
static THREAD_LOCAL const char** tls_bulk_values = NULL;
static THREAD_LOCAL Py_ssize_t tls_bulk_values_cap = 0;
static THREAD_LOCAL uint64_t* tls_bulk_value_hashes = NULL;
static THREAD_LOCAL uint64_t* tls_bulk_value_prefixes = NULL;
static THREAD_LOCAL uint32_t* tls_bulk_value_lens = NULL;
static THREAD_LOCAL Py_ssize_t tls_bulk_value_meta_cap = 0;

static PyObject* method_bulk_insert_keys(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    if (global_reads_sealed) {
        PyErr_SetString(PyExc_RuntimeError, "LQFT reads are sealed for lock-free mode; unseal before writing");
        return NULL;
    }
    PyObject* seq = PySequence_Fast(args[0], "bulk_insert_keys expects a sequence of string keys");
    if (!seq) return NULL;

    Py_ssize_t val_len_ssize = 0;
    const char* val_str = PyUnicode_AsUTF8AndSize(args[1], &val_len_ssize);
    if (!val_str) {
        Py_DECREF(seq);
        return NULL;
    }
    uint32_t val_len = (uint32_t)val_len_ssize;
    uint64_t val_hash = hash_bytes_64(val_str, val_len);
    uint64_t pre = build_leaf_prefix_hash(val_str, val_len);

    Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
    if (n <= 0) {
        Py_DECREF(seq);
        Py_RETURN_NONE;
    }

    if (n > tls_bulk_hashes_cap) {
        uint64_t* grown = (uint64_t*)realloc(tls_bulk_hashes, (size_t)n * sizeof(uint64_t));
        if (!grown) {
            Py_DECREF(seq);
            return PyErr_NoMemory();
        }
        tls_bulk_hashes = grown;
        tls_bulk_hashes_cap = n;
    }
    uint64_t* hashes = tls_bulk_hashes;

    PyObject** items = PySequence_Fast_ITEMS(seq);
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* key_str = PyUnicode_AsUTF8(items[i]);
        if (!key_str) {
            Py_DECREF(seq);
            return NULL;
        }
        hashes[i] = hash_key_string(key_str);
    }

    Py_BEGIN_ALLOW_THREADS
    for (Py_ssize_t i = 0; i < n; i++) {
        c_internal_insert_rw_precomputed(hashes[i], val_str, pre, val_hash, val_len);
    }
    Py_END_ALLOW_THREADS

    Py_DECREF(seq);
    Py_RETURN_NONE;
}

static PyObject* method_bulk_insert_key_values(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    if (global_reads_sealed) {
        PyErr_SetString(PyExc_RuntimeError, "LQFT reads are sealed for lock-free mode; unseal before writing");
        return NULL;
    }

    PyObject* key_seq = PySequence_Fast(args[0], "bulk_insert_key_values expects a sequence of string keys");
    if (!key_seq) return NULL;
    PyObject* value_seq = PySequence_Fast(args[1], "bulk_insert_key_values expects a sequence of string values");
    if (!value_seq) {
        Py_DECREF(key_seq);
        return NULL;
    }

    Py_ssize_t n = PySequence_Fast_GET_SIZE(key_seq);
    if (n != PySequence_Fast_GET_SIZE(value_seq)) {
        Py_DECREF(key_seq);
        Py_DECREF(value_seq);
        PyErr_SetString(PyExc_ValueError, "bulk_insert_key_values expects matching key/value lengths");
        return NULL;
    }
    if (n <= 0) {
        Py_DECREF(key_seq);
        Py_DECREF(value_seq);
        Py_RETURN_NONE;
    }

    if (n > tls_bulk_hashes_cap) {
        uint64_t* grown = (uint64_t*)realloc(tls_bulk_hashes, (size_t)n * sizeof(uint64_t));
        if (!grown) {
            Py_DECREF(key_seq);
            Py_DECREF(value_seq);
            return PyErr_NoMemory();
        }
        tls_bulk_hashes = grown;
        tls_bulk_hashes_cap = n;
    }
    if (n > tls_bulk_values_cap) {
        const char** grown = (const char**)realloc((void*)tls_bulk_values, (size_t)n * sizeof(const char*));
        if (!grown) {
            Py_DECREF(key_seq);
            Py_DECREF(value_seq);
            return PyErr_NoMemory();
        }
        tls_bulk_values = grown;
        tls_bulk_values_cap = n;
    }
    if (n > tls_bulk_value_meta_cap) {
        uint64_t* grown_hashes = (uint64_t*)realloc(tls_bulk_value_hashes, (size_t)n * sizeof(uint64_t));
        if (!grown_hashes) {
            Py_DECREF(key_seq);
            Py_DECREF(value_seq);
            return PyErr_NoMemory();
        }
        tls_bulk_value_hashes = grown_hashes;
        uint64_t* grown_prefixes = (uint64_t*)realloc(tls_bulk_value_prefixes, (size_t)n * sizeof(uint64_t));
        if (!grown_prefixes) {
            Py_DECREF(key_seq);
            Py_DECREF(value_seq);
            return PyErr_NoMemory();
        }
        tls_bulk_value_prefixes = grown_prefixes;
        uint32_t* grown_lens = (uint32_t*)realloc(tls_bulk_value_lens, (size_t)n * sizeof(uint32_t));
        if (!grown_lens) {
            Py_DECREF(key_seq);
            Py_DECREF(value_seq);
            return PyErr_NoMemory();
        }
        tls_bulk_value_lens = grown_lens;
        tls_bulk_value_meta_cap = n;
    }

    uint64_t* hashes = tls_bulk_hashes;
    const char** values = tls_bulk_values;
    uint64_t* value_hashes = tls_bulk_value_hashes;
    uint64_t* value_prefixes = tls_bulk_value_prefixes;
    uint32_t* value_lens = tls_bulk_value_lens;
    PyObject** key_items = PySequence_Fast_ITEMS(key_seq);
    PyObject** value_items = PySequence_Fast_ITEMS(value_seq);
    for (Py_ssize_t i = 0; i < n; i++) {
        Py_ssize_t key_len = 0;
        Py_ssize_t value_len_ssize = 0;
        const char* key_str = PyUnicode_AsUTF8AndSize(key_items[i], &key_len);
        const char* value_str = PyUnicode_AsUTF8AndSize(value_items[i], &value_len_ssize);
        if (!key_str || !value_str) {
            Py_DECREF(key_seq);
            Py_DECREF(value_seq);
            return NULL;
        }
        hashes[i] = hash_key_bytes(key_str, key_len);
        values[i] = value_str;
        value_lens[i] = (uint32_t)value_len_ssize;
        value_hashes[i] = hash_bytes_64(value_str, value_lens[i]);
        value_prefixes[i] = build_leaf_prefix_hash(value_str, value_lens[i]);
    }

    Py_BEGIN_ALLOW_THREADS
    for (Py_ssize_t i = 0; i < n; i++) {
        c_internal_insert_rw_precomputed(hashes[i], values[i], value_prefixes[i], value_hashes[i], value_lens[i]);
    }
    Py_END_ALLOW_THREADS

    Py_DECREF(key_seq);
    Py_DECREF(value_seq);
    Py_RETURN_NONE;
}

static PyObject* method_bulk_insert_range(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 4) return NULL;
    if (global_reads_sealed) {
        PyErr_SetString(PyExc_RuntimeError, "LQFT reads are sealed for lock-free mode; unseal before writing");
        return NULL;
    }
    const char* prefix = PyUnicode_AsUTF8(args[0]);
    if (!prefix) return NULL;
    unsigned long long start = PyLong_AsUnsignedLongLong(args[1]);
    if (PyErr_Occurred()) return NULL;
    unsigned long long count = PyLong_AsUnsignedLongLong(args[2]);
    if (PyErr_Occurred()) return NULL;
    Py_ssize_t val_len_ssize = 0;
    const char* val_str = PyUnicode_AsUTF8AndSize(args[3], &val_len_ssize);
    if (!val_str) return NULL;
    uint32_t val_len = (uint32_t)val_len_ssize;
    uint64_t val_hash = hash_bytes_64(val_str, val_len);
    uint64_t pre = build_leaf_prefix_hash(val_str, val_len);

    uint64_t prefix_hash = fnv1a_update(FNV_OFFSET_BASIS, prefix, strlen(prefix));

    Py_BEGIN_ALLOW_THREADS
    for (unsigned long long i = 0; i < count; i++) {
        uint64_t h = fnv1a_update_u64_decimal(prefix_hash, (uint64_t)(start + i));
        c_internal_insert_rw_precomputed(h, val_str, pre, val_hash, val_len);
    }
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* method_search(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    uint64_t h = PyLong_AsUnsignedLongLongMask(args[0]);
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    PyObject* py_res = NULL;
    if (global_reads_sealed) {
        LQFTNode* current_root = global_roots[shard].root;
        const char* result_ptr = NULL;
        if (current_root) result_ptr = core_search(h, current_root);
        if (result_ptr) return PyUnicode_FromString(result_ptr);
        Py_RETURN_NONE;
    }

    LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
    LQFTNode* current_root = global_roots[shard].root;
    if (current_root) {
        const char* result_ptr = core_search(h, current_root);
        if (result_ptr) py_res = PyUnicode_FromString(result_ptr);
    }
    LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);

    if (py_res) return py_res;

    Py_RETURN_NONE;
}

static PyObject* method_search_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    Py_ssize_t key_len = 0;
    const char* key_str = PyUnicode_AsUTF8AndSize(args[0], &key_len);
    if (!key_str) return NULL;
    uint64_t h = hash_key_unicode_cached(args[0], key_str, key_len);
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    PyObject* py_res = NULL;
    if (global_reads_sealed) {
        LQFTNode* current_root = global_roots[shard].root;
        const char* result_ptr = NULL;
        if (current_root) result_ptr = core_search(h, current_root);
        if (result_ptr) return PyUnicode_FromString(result_ptr);
        Py_RETURN_NONE;
    }

    LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
    LQFTNode* current_root = global_roots[shard].root;
    if (current_root) {
        const char* result_ptr = core_search(h, current_root);
        if (result_ptr) py_res = PyUnicode_FromString(result_ptr);
    }
    LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);

    if (py_res) return py_res;

    Py_RETURN_NONE;
}

static PyObject* method_contains(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    uint64_t h = PyLong_AsUnsignedLongLongMask(args[0]);
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    int found = 0;
    if (global_reads_sealed) {
        LQFTNode* current_root = global_roots[shard].root;
        if (current_root && core_search(h, current_root) != NULL) found = 1;
        if (found) Py_RETURN_TRUE;
        Py_RETURN_FALSE;
    }

    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
    LQFTNode* current_root = global_roots[shard].root;
    if (current_root) {
        if (core_search(h, current_root) != NULL) found = 1;
    }
    LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
    Py_END_ALLOW_THREADS
    if (found) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject* method_contains_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    Py_ssize_t key_len = 0;
    const char* key_str = PyUnicode_AsUTF8AndSize(args[0], &key_len);
    if (!key_str) return NULL;
    uint64_t h = hash_key_unicode_cached(args[0], key_str, key_len);
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    int found = 0;
    if (global_reads_sealed) {
        LQFTNode* current_root = global_roots[shard].root;
        if (current_root && core_search(h, current_root) != NULL) found = 1;
        if (found) Py_RETURN_TRUE;
        Py_RETURN_FALSE;
    }

    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
    LQFTNode* current_root = global_roots[shard].root;
    if (current_root) {
        if (core_search(h, current_root) != NULL) found = 1;
    }
    LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
    Py_END_ALLOW_THREADS
    if (found) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject* method_bulk_contains_count(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    PyObject* seq = PySequence_Fast(args[0], "bulk_contains_count expects a sequence of string keys");
    if (!seq) return NULL;

    Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
    if (n <= 0) {
        Py_DECREF(seq);
        return PyLong_FromLong(0);
    }

    if (n > tls_bulk_hashes_cap) {
        uint64_t* grown = (uint64_t*)realloc(tls_bulk_hashes, (size_t)n * sizeof(uint64_t));
        if (!grown) {
            Py_DECREF(seq);
            return PyErr_NoMemory();
        }
        tls_bulk_hashes = grown;
        tls_bulk_hashes_cap = n;
    }
    uint64_t* hashes = tls_bulk_hashes;

    PyObject** items = PySequence_Fast_ITEMS(seq);
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* key_str = PyUnicode_AsUTF8(items[i]);
        if (!key_str) {
            Py_DECREF(seq);
            return NULL;
        }
        hashes[i] = hash_key_string(key_str);
    }

    Py_ssize_t hit_count = 0;
    if (global_reads_sealed) {
        Py_BEGIN_ALLOW_THREADS
        for (Py_ssize_t i = 0; i < n; i++) {
            uint64_t h = hashes[i];
            uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
            LQFTNode* current_root = global_roots[shard].root;
            if (current_root) {
                if (core_search(h, current_root) != NULL) hit_count++;
            }
        }
        Py_END_ALLOW_THREADS
        Py_DECREF(seq);
        return PyLong_FromSsize_t(hit_count);
    }

    Py_BEGIN_ALLOW_THREADS
    for (Py_ssize_t i = 0; i < n; i++) {
        uint64_t h = hashes[i];
        uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);

        LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
        LQFTNode* current_root = global_roots[shard].root;

        if (current_root) {
            if (core_search(h, current_root) != NULL) hit_count++;
        }
        LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
    }
    Py_END_ALLOW_THREADS

    Py_DECREF(seq);
    return PyLong_FromSsize_t(hit_count);
}

static PyObject* method_bulk_contains_range_count(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 3) return NULL;
    const char* prefix = PyUnicode_AsUTF8(args[0]);
    if (!prefix) return NULL;
    unsigned long long start = PyLong_AsUnsignedLongLong(args[1]);
    if (PyErr_Occurred()) return NULL;
    unsigned long long count = PyLong_AsUnsignedLongLong(args[2]);
    if (PyErr_Occurred()) return NULL;

    uint64_t prefix_hash = fnv1a_update(FNV_OFFSET_BASIS, prefix, strlen(prefix));
    unsigned long long hit_count = 0;

    if (global_reads_sealed) {
        Py_BEGIN_ALLOW_THREADS
        for (unsigned long long i = 0; i < count; i++) {
            uint64_t h = fnv1a_update_u64_decimal(prefix_hash, (uint64_t)(start + i));
            uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
            LQFTNode* current_root = global_roots[shard].root;
            if (current_root) {
                if (core_search(h, current_root) != NULL) hit_count++;
            }
        }
        Py_END_ALLOW_THREADS
        return PyLong_FromUnsignedLongLong(hit_count);
    }

    Py_BEGIN_ALLOW_THREADS
    for (unsigned long long i = 0; i < count; i++) {
        uint64_t h = fnv1a_update_u64_decimal(prefix_hash, (uint64_t)(start + i));
        uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);

        LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
        LQFTNode* current_root = global_roots[shard].root;

        if (current_root) {
            if (core_search(h, current_root) != NULL) hit_count++;
        }
        LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
    }
    Py_END_ALLOW_THREADS

    return PyLong_FromUnsignedLongLong(hit_count);
}

static PyObject* method_delete(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    if (global_reads_sealed) {
        PyErr_SetString(PyExc_RuntimeError, "LQFT reads are sealed for lock-free mode; unseal before deleting");
        return NULL;
    }
    uint64_t h = PyLong_AsUnsignedLongLongMask(args[0]);
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    Py_BEGIN_ALLOW_THREADS
    while(1) {
        LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
        LQFTNode* old_root = global_roots[shard].root;
        if (old_root) ATOMIC_INC(&old_root->ref_count);
        LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
        LQFTNode* next = core_delete_internal(h, old_root);
        LQFT_RWLOCK_WRLOCK(&root_locks[shard].lock);
        if (global_roots[shard].root == old_root) {
            global_roots[shard].root = next; 
            LQFT_RWLOCK_UNLOCK_WR(&root_locks[shard].lock);
            if (old_root) { decref(old_root); decref(old_root); }
            break;
        } else {
            LQFT_RWLOCK_UNLOCK_WR(&root_locks[shard].lock);
            if (next) decref(next);
            if (old_root) decref(old_root);
            for(volatile int s = 0; s < 16; s++) { CPU_PAUSE; }
        }
    }
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* method_delete_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    if (global_reads_sealed) {
        PyErr_SetString(PyExc_RuntimeError, "LQFT reads are sealed for lock-free mode; unseal before deleting");
        return NULL;
    }
    Py_ssize_t key_len = 0;
    const char* key_str = PyUnicode_AsUTF8AndSize(args[0], &key_len);
    if (!key_str) return NULL;
    uint64_t h = hash_key_unicode_cached(args[0], key_str, key_len);
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    Py_BEGIN_ALLOW_THREADS
    while(1) {
        LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
        LQFTNode* old_root = global_roots[shard].root;
        if (old_root) ATOMIC_INC(&old_root->ref_count);
        LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
        LQFTNode* next = core_delete_internal(h, old_root);
        LQFT_RWLOCK_WRLOCK(&root_locks[shard].lock);
        if (global_roots[shard].root == old_root) {
            global_roots[shard].root = next;
            LQFT_RWLOCK_UNLOCK_WR(&root_locks[shard].lock);
            if (old_root) { decref(old_root); decref(old_root); }
            break;
        } else {
            LQFT_RWLOCK_UNLOCK_WR(&root_locks[shard].lock);
            if (next) decref(next);
            if (old_root) decref(old_root);
            for(volatile int s = 0; s < 16; s++) { CPU_PAUSE; }
        }
    }
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* method_get_metrics(PyObject* self, PyObject* args) { 
    int64_t total_phys_added = 0;
    int64_t total_phys_freed = 0;
    int64_t total_logical = 0;
    int64_t total_child_bytes_added = 0;
    int64_t total_child_bytes_freed = 0;
    for (int i = 0; i < MAX_TRACKED_THREADS; i++) {
        total_phys_added += global_metrics_array[i].phys_added;
        total_phys_freed += global_metrics_array[i].phys_freed;
        total_logical += global_metrics_array[i].logical_inserts;
        total_child_bytes_added += global_metrics_array[i].child_bytes_added;
        total_child_bytes_freed += global_metrics_array[i].child_bytes_freed;
    }
    int64_t net_phys = total_phys_added - total_phys_freed;
    int64_t net_child_bytes = total_child_bytes_added - total_child_bytes_freed;
    double deduplication_ratio = net_phys > 0 ? (double)total_logical / (double)net_phys : 0.0;
    double value_pool_bytes_per_logical_insert = total_logical > 0
        ? (double)value_pool_total_bytes / (double)total_logical
        : 0.0;
    int64_t estimated_native_bytes =
        net_phys * (int64_t)sizeof(LQFTNode) +
        net_child_bytes +
        value_pool_total_bytes;
    double bytes_per_physical_node = net_phys > 0
        ? (double)estimated_native_bytes / (double)net_phys
        : 0.0;
    return Py_BuildValue(
        "{s:L, s:L, s:d, s:L, s:L, s:d, s:L, s:L, s:d, s:i, s:i}",
        "physical_nodes", net_phys,
        "logical_inserts", total_logical,
        "deduplication_ratio", deduplication_ratio,
        "value_pool_entries", value_pool_entry_count,
        "value_pool_bytes", value_pool_total_bytes,
        "value_pool_bytes_per_logical_insert", value_pool_bytes_per_logical_insert,
        "active_child_bytes", net_child_bytes,
        "estimated_native_bytes", estimated_native_bytes,
        "bytes_per_physical_node", bytes_per_physical_node,
        "sizeof_lqft_node", (int)sizeof(LQFTNode),
        "sizeof_child_pointer", (int)sizeof(LQFTNode*)
    );
}

static PyObject* method_free_all(PyObject* self, PyObject* args) {
    if (global_reads_sealed) {
        PyErr_SetString(PyExc_RuntimeError, "LQFT reads are sealed for lock-free mode; unseal before clearing");
        return NULL;
    }
    Py_BEGIN_ALLOW_THREADS
    for(int i = 0; i < NUM_ROOTS; i++) LQFT_RWLOCK_WRLOCK(&root_locks[i].lock);
    for(int i = 0; i < NUM_STRIPES; i++) fast_lock_backoff(&stripe_locks[i].flag);
    if (registry) {
        for(int i = 0; i < NUM_STRIPES * STRIPE_SIZE; i++) {
            registry[i] = NULL;
        }
    }
    value_pool_clear_all();
    fast_lock_backoff(&global_chunk_lock.flag);
    NodeChunk* nc = global_node_chunks;
    while(nc) { NodeChunk* n = nc->next_global; free_node_chunk(nc); nc = n; }
    global_node_chunks = NULL;

    nc = pre_zeroed_node_chunks;
    while(nc) { NodeChunk* n = nc->next_global; free_node_chunk(nc); nc = n; }
    pre_zeroed_node_chunks = NULL;
    pre_node_count = 0;

    node_pool.head = NULL;
    fast_unlock(&global_chunk_lock.flag);
    for (int i = 0; i < MAX_TRACKED_THREADS; i++) {
        global_metrics_array[i].phys_added = 0;
        global_metrics_array[i].phys_freed = 0;
        global_metrics_array[i].logical_inserts = 0;
        global_metrics_array[i].child_bytes_added = 0;
        global_metrics_array[i].child_bytes_freed = 0;
    }
#ifdef _MSC_VER
    _InterlockedExchange(&registered_threads_count, 0);
    _InterlockedIncrement(&global_arena_epoch);
#else
    __sync_lock_test_and_set(&registered_threads_count, 0);
    __sync_add_and_fetch(&global_arena_epoch, 1);
#endif
    for(int i = NUM_STRIPES - 1; i >= 0; i--) fast_unlock(&stripe_locks[i].flag);
    for(int i = NUM_ROOTS - 1; i >= 0; i--) { 
        global_roots[i].root = NULL; 
        LQFT_RWLOCK_UNLOCK_WR(&root_locks[i].lock); 
    }
    Py_END_ALLOW_THREADS 
    Py_RETURN_NONE;
}

static PyMethodDef LQFTMethods[] = {
    {"mutable_new", method_mutable_new, METH_NOARGS, "Create native mutable hash table state"},
    {"mutable_insert_key_value", (PyCFunction)method_mutable_insert_key_value, METH_FASTCALL, "Insert into native mutable hash table"},
    {"mutable_search_key", (PyCFunction)method_mutable_search_key, METH_FASTCALL, "Search native mutable hash table by string key"},
    {"mutable_contains_key", (PyCFunction)method_mutable_contains_key, METH_FASTCALL, "Contains check in native mutable hash table"},
    {"mutable_delete_key", (PyCFunction)method_mutable_delete_key, METH_FASTCALL, "Delete from native mutable hash table"},
    {"mutable_clear", (PyCFunction)method_mutable_clear, METH_FASTCALL, "Clear native mutable hash table"},
    {"mutable_len", (PyCFunction)method_mutable_len, METH_FASTCALL, "Length of native mutable hash table"},
    {"mutable_get_metrics", (PyCFunction)method_mutable_get_metrics, METH_FASTCALL, "Metrics for native mutable hash table"},
    {"mutable_export_items", (PyCFunction)method_mutable_export_items, METH_FASTCALL, "Export native mutable hash table items"},
    {"insert", (PyCFunction)method_insert, METH_FASTCALL, "Fast-path insert single key"},
    {"insert_key_value", (PyCFunction)method_insert_key_value, METH_FASTCALL, "Insert using string key/value fast path"},
    {"bulk_insert_keys", (PyCFunction)method_bulk_insert_keys, METH_FASTCALL, "Bulk insert string keys with one value"},
    {"bulk_insert_key_values", (PyCFunction)method_bulk_insert_key_values, METH_FASTCALL, "Bulk insert paired string keys and values"},
    {"bulk_insert_range", (PyCFunction)method_bulk_insert_range, METH_FASTCALL, "Bulk insert generated keys prefix+index range"},
    {"search", (PyCFunction)method_search, METH_FASTCALL, "Fast-path search single key"},
    {"search_key", (PyCFunction)method_search_key, METH_FASTCALL, "Search using string key fast path"},
    {"contains", (PyCFunction)method_contains, METH_FASTCALL, "Contains check by pre-hashed key"},
    {"contains_key", (PyCFunction)method_contains_key, METH_FASTCALL, "Contains check using string key fast path"},
    {"bulk_contains_count", (PyCFunction)method_bulk_contains_count, METH_FASTCALL, "Bulk contains checks, returns hit count"},
    {"bulk_contains_range_count", (PyCFunction)method_bulk_contains_range_count, METH_FASTCALL, "Bulk contains on generated keys prefix+index range"},
    {"set_reads_sealed", (PyCFunction)method_set_reads_sealed, METH_FASTCALL, "Enable or disable lock-free sealed read mode"},
    {"delete", (PyCFunction)method_delete, METH_FASTCALL, "Fast-path delete single key"},
    {"delete_key", (PyCFunction)method_delete_key, METH_FASTCALL, "Delete using string key fast path"},
    {"internal_stress_test", (PyCFunction)method_internal_stress_test, METH_FASTCALL, "Run native C stress test"},
    {"get_metrics", method_get_metrics, METH_VARARGS, "Get stats"},
    {"free_all", method_free_all, METH_VARARGS, "Wipe memory"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef lqftmodule = { PyModuleDef_HEAD_INIT, "lqft_c_engine", NULL, -1, LQFTMethods };

PyMODINIT_FUNC PyInit_lqft_c_engine(void) { 
    PyObject* module;
    for(int i = 0; i < NUM_ROOTS; i++) {
        global_roots[i].root = NULL;
        LQFT_RWLOCK_INIT(&root_locks[i].lock);
    }
    registry = (LQFTNode**)calloc(NUM_STRIPES * STRIPE_SIZE, sizeof(LQFTNode*));
    for(int i = 0; i < NUM_STRIPES; i++) stripe_locks[i].flag = 0;
    for(int i = 0; i < VALUE_POOL_BUCKETS; i++) value_pool_locks[i].flag = 0;
    for(int j = 0; j < 4; j++) {
        NodeChunk* nc = alloc_node_chunk();
        if (nc) { memset(nc, 0, sizeof(NodeChunk)); nc->next_global = pre_zeroed_node_chunks; pre_zeroed_node_chunks = nc; pre_node_count++; }
    }
    bg_alloc_running = 1;
#ifdef _MSC_VER
    CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)background_alloc_thread, NULL, 0, NULL);
#else
    pthread_t bg_tid; pthread_create(&bg_tid, NULL, background_alloc_thread, NULL); pthread_detach(bg_tid); 
#endif
    if (PyType_Ready(&NativeMutableLQFTType) < 0) return NULL;
    module = PyModule_Create(&lqftmodule);
    if (!module) return NULL;
    Py_INCREF(&NativeMutableLQFTType);
    if (PyModule_AddObject(module, "NativeMutableLQFT", (PyObject*)&NativeMutableLQFTType) < 0) {
        Py_DECREF(&NativeMutableLQFTType);
        Py_DECREF(module);
        return NULL;
    }
    return module; 
}

static const char* value_acquire(const char* value_ptr, uint64_t value_hash, uint32_t value_len) {
    if (!value_ptr) return NULL;

    uint32_t bucket = (uint32_t)(value_hash & (VALUE_POOL_BUCKETS - 1));

    fast_lock_backoff(&value_pool_locks[bucket].flag);
    ValueEntry* cur = value_pool[bucket];
    while (cur) {
        if (cur->hash == value_hash && cur->len == value_len && strcmp(cur->str, value_ptr) == 0) {
            ATOMIC_INC(&cur->ref_count);
            fast_unlock(&value_pool_locks[bucket].flag);
            return cur->str;
        }
        cur = cur->next;
    }

    ValueEntry* e = (ValueEntry*)malloc(sizeof(ValueEntry));
    if (!e) {
        fast_unlock(&value_pool_locks[bucket].flag);
        return NULL;
    }
    e->str = portable_strdup(value_ptr);
    if (!e->str) {
        free(e);
        fast_unlock(&value_pool_locks[bucket].flag);
        return NULL;
    }
    e->hash = value_hash;
    e->len = value_len;
    e->ref_count = 1;
    e->next = value_pool[bucket];
    value_pool[bucket] = e;
    value_pool_entry_count += 1;
    value_pool_total_bytes += (int64_t)(value_len + 1);
    fast_unlock(&value_pool_locks[bucket].flag);
    return e->str;
}

static void value_release(const char* value_ptr, uint64_t value_hash) {
    if (!value_ptr) return;

    uint32_t bucket = (uint32_t)(value_hash & (VALUE_POOL_BUCKETS - 1));

    fast_lock_backoff(&value_pool_locks[bucket].flag);
    ValueEntry* prev = NULL;
    ValueEntry* cur = value_pool[bucket];
    while (cur) {
        if (cur->hash == value_hash && (cur->str == value_ptr || strcmp(cur->str, value_ptr) == 0)) {
            long new_ref = ATOMIC_DEC(&cur->ref_count);
            if (new_ref == 0) {
                if (prev) prev->next = cur->next;
                else value_pool[bucket] = cur->next;
                value_pool_entry_count -= 1;
                value_pool_total_bytes -= (int64_t)(cur->len + 1);
                free(cur->str);
                free(cur);
            }
            fast_unlock(&value_pool_locks[bucket].flag);
            return;
        }
        prev = cur;
        cur = cur->next;
    }
    fast_unlock(&value_pool_locks[bucket].flag);
}

static void value_pool_clear_all(void) {
    for (uint32_t b = 0; b < VALUE_POOL_BUCKETS; b++) {
        fast_lock_backoff(&value_pool_locks[b].flag);
        ValueEntry* cur = value_pool[b];
        while (cur) {
            ValueEntry* next = cur->next;
            free(cur->str);
            free(cur);
            cur = next;
        }
        value_pool[b] = NULL;
        fast_unlock(&value_pool_locks[b].flag);
    }
    value_pool_entry_count = 0;
    value_pool_total_bytes = 0;
}

static inline char* copy_bytes_with_nul(const char* src, Py_ssize_t len) {
    char* dst = (char*)malloc((size_t)len + 1u);
    if (!dst) return NULL;
    memcpy(dst, src, (size_t)len);
    dst[len] = '\0';
    return dst;
}

static inline size_t mutable_next_pow2(size_t value) {
    size_t cap = 1024;
    while (cap < value) cap <<= 1;
    return cap;
}

#define MUTABLE_TABLE_CAPSULE_NAME "lqft_c_engine.MutableTable"

static MutableTable* mutable_table_from_capsule(PyObject* capsule) {
    return (MutableTable*)PyCapsule_GetPointer(capsule, MUTABLE_TABLE_CAPSULE_NAME);
}

static void mutable_table_capsule_destructor(PyObject* capsule) {
    MutableTable* table_state = mutable_table_from_capsule(capsule);
    if (!table_state) {
        PyErr_Clear();
        return;
    }
    mutable_clear_all(table_state);
    free(table_state);
}

static inline void mutable_entry_reset(MutableEntry* entry) {
    entry->key_obj = NULL;
    entry->value_obj = NULL;
    entry->key_utf8 = NULL;
    entry->hash = 0;
    entry->key_len = 0;
    entry->fingerprint = 0;
    entry->state = MUTABLE_EMPTY;
}

static inline void mutable_entry_release(MutableEntry* entry) {
    if (entry->state == MUTABLE_OCCUPIED) {
        Py_DECREF(entry->key_obj);
        Py_DECREF(entry->value_obj);
    }
    entry->key_obj = NULL;
    entry->value_obj = NULL;
    entry->key_utf8 = NULL;
    entry->hash = 0;
    entry->key_len = 0;
    entry->fingerprint = 0;
}

static inline uint8_t mutable_hash_fingerprint(uint64_t hash) {
    uint8_t fp = (uint8_t)((hash >> 57) & 0x7Fu);
    return (uint8_t)(fp + 1u);
}

static size_t mutable_find_slot(MutableEntry* table, size_t capacity, const char* key, Py_ssize_t key_len, uint64_t hash, int* found) {
    size_t mask = capacity - 1u;
    size_t idx = (size_t)hash & mask;
    size_t first_deleted = (size_t)-1;
    uint8_t fingerprint = mutable_hash_fingerprint(hash);
    for (;;) {
        MutableEntry* entry = &table[idx];
        if (entry->state == MUTABLE_EMPTY) {
            *found = 0;
            return first_deleted != (size_t)-1 ? first_deleted : idx;
        }
        if (entry->state == MUTABLE_DELETED) {
            if (first_deleted == (size_t)-1) first_deleted = idx;
        } else if (entry->fingerprint == fingerprint && entry->hash == hash && entry->key_len == key_len && memcmp(entry->key_utf8, key, (size_t)key_len) == 0) {
            *found = 1;
            return idx;
        }
        idx = (idx + 1u) & mask;
    }
}

static inline size_t mutable_probe_distance(size_t slot, size_t home, size_t mask) {
    return (slot - home) & mask;
}

static int mutable_resize(MutableTable* table_state, size_t min_capacity) {
    size_t new_capacity = mutable_next_pow2(min_capacity);
    MutableEntry* new_table = (MutableEntry*)calloc(new_capacity, sizeof(MutableEntry));
    if (!new_table) return -1;

    if (table_state->table) {
        for (size_t i = 0; i < table_state->capacity; i++) {
            MutableEntry* entry = &table_state->table[i];
            if (entry->state != MUTABLE_OCCUPIED) continue;
            int found = 0;
            size_t idx = mutable_find_slot(new_table, new_capacity, entry->key_utf8, entry->key_len, entry->hash, &found);
            MutableEntry* dst = &new_table[idx];
            *dst = *entry;
        }
        free(table_state->table);
    }

    table_state->table = new_table;
    table_state->capacity = new_capacity;
    table_state->used = table_state->size;
    table_state->tombstones = 0;
    return 0;
}

static void mutable_delete_entry_at(MutableTable* table_state, MutableEntry* entry) {
    size_t mask = table_state->capacity - 1u;
    size_t hole = (size_t)(entry - table_state->table);
    size_t scan = (hole + 1u) & mask;

    mutable_entry_release(entry);
    table_state->size--;

    while (table_state->table[scan].state == MUTABLE_OCCUPIED) {
        MutableEntry* current = &table_state->table[scan];
        size_t home = (size_t)current->hash & mask;
        size_t current_distance = mutable_probe_distance(scan, home, mask);
        size_t hole_distance = mutable_probe_distance(hole, home, mask);

        if (hole_distance < current_distance) {
            table_state->table[hole] = *current;
            mutable_entry_reset(current);
            hole = scan;
        }
        scan = (scan + 1u) & mask;
    }

    mutable_entry_reset(&table_state->table[hole]);
    table_state->used--;
    table_state->tombstones = 0;
}

static int mutable_ensure_capacity(MutableTable* table_state, size_t extra) {
    if (table_state->capacity == 0) {
        return mutable_resize(table_state, 1024);
    }
    size_t required_used = table_state->used + extra;
    if (required_used * 10u < table_state->capacity * 7u) return 0;
    return mutable_resize(table_state, table_state->capacity << 1u);
}

static void mutable_clear_all(MutableTable* table_state) {
    if (!table_state || !table_state->table) return;
    for (size_t i = 0; i < table_state->capacity; i++) {
        mutable_entry_release(&table_state->table[i]);
    }
    free(table_state->table);
    table_state->table = NULL;
    table_state->capacity = 0;
    table_state->size = 0;
    table_state->used = 0;
    table_state->tombstones = 0;
}

static PyObject* mutable_build_metrics(MutableTable* table_state) {
    return Py_BuildValue(
        "{s:n, s:n, s:s, s:n, s:n}",
        "logical_inserts", (Py_ssize_t)table_state->size,
        "physical_nodes", (Py_ssize_t)table_state->size,
        "frontend", "native-mutable-hashtable",
        "mutable_capacity", (Py_ssize_t)table_state->capacity,
        "mutable_tombstones", (Py_ssize_t)table_state->tombstones
    );
}

static PyObject* mutable_export_items_from_table(MutableTable* table_state) {
    PyObject* keys = PyList_New((Py_ssize_t)table_state->size);
    if (!keys) return NULL;
    PyObject* values = PyList_New((Py_ssize_t)table_state->size);
    if (!values) {
        Py_DECREF(keys);
        return NULL;
    }
    Py_ssize_t out_idx = 0;
    for (size_t i = 0; i < table_state->capacity; i++) {
        MutableEntry* entry = &table_state->table[i];
        if (entry->state != MUTABLE_OCCUPIED) continue;
        PyObject* py_key = Py_NewRef(entry->key_obj);
        PyObject* py_value = Py_NewRef(entry->value_obj);
        if (!py_key || !py_value) {
            Py_XDECREF(py_key);
            Py_XDECREF(py_value);
            Py_DECREF(keys);
            Py_DECREF(values);
            return NULL;
        }
        PyList_SET_ITEM(keys, out_idx, py_key);
        PyList_SET_ITEM(values, out_idx, py_value);
        out_idx++;
    }
    return Py_BuildValue("NN", keys, values);
}

static int mutable_insert_object(MutableTable* table_state, PyObject* key_obj, const char* key, Py_ssize_t key_len, PyObject* value_obj) {
    uint64_t hash = hash_key_bytes(key, key_len);
    if (mutable_ensure_capacity(table_state, 1) != 0) return -1;
    int found = 0;
    size_t idx = mutable_find_slot(table_state->table, table_state->capacity, key, key_len, hash, &found);
    MutableEntry* entry = &table_state->table[idx];
    Py_INCREF(value_obj);

    if (found) {
        Py_DECREF(entry->value_obj);
        entry->value_obj = value_obj;
        return 0;
    }

    Py_INCREF(key_obj);

    if (entry->state == MUTABLE_EMPTY) table_state->used++;
    entry->key_obj = key_obj;
    entry->value_obj = value_obj;
    entry->key_utf8 = key;
    entry->hash = hash;
    entry->key_len = key_len;
    entry->fingerprint = mutable_hash_fingerprint(hash);
    entry->state = MUTABLE_OCCUPIED;
    table_state->size++;
    return 0;
}

static PyObject* native_mutable_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    NativeMutableLQFTObject* self = (NativeMutableLQFTObject*)type->tp_alloc(type, 0);
    if (!self) return NULL;
    memset(&self->table_state, 0, sizeof(self->table_state));
    return (PyObject*)self;
}

static int native_mutable_init(NativeMutableLQFTObject* self, PyObject* args, PyObject* kwds) {
    return 0;
}

static void native_mutable_dealloc(NativeMutableLQFTObject* self) {
    mutable_clear_all(&self->table_state);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* native_mutable_insert(NativeMutableLQFTObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    Py_ssize_t key_len = 0;
    const char* key = PyUnicode_AsUTF8AndSize(args[0], &key_len);
    if (!key) return NULL;
    if (!PyUnicode_Check(args[1])) {
        PyErr_Format(PyExc_TypeError, "LQFT values must be strings. Received: %s", Py_TYPE(args[1])->tp_name);
        return NULL;
    }
    if (mutable_insert_object(&self->table_state, args[0], key, key_len, args[1]) != 0) return PyErr_NoMemory();
    Py_RETURN_NONE;
}

static PyObject* native_mutable_search(NativeMutableLQFTObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    Py_ssize_t key_len = 0;
    const char* key = PyUnicode_AsUTF8AndSize(args[0], &key_len);
    if (!key) return NULL;
    MutableEntry* entry = mutable_lookup_entry(&self->table_state, key, key_len);
    if (!entry) Py_RETURN_NONE;
    return Py_NewRef(entry->value_obj);
}

static PyObject* native_mutable_contains(NativeMutableLQFTObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    Py_ssize_t key_len = 0;
    const char* key = PyUnicode_AsUTF8AndSize(args[0], &key_len);
    if (!key) return NULL;
    if (mutable_lookup_entry(&self->table_state, key, key_len)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject* native_mutable_delete(NativeMutableLQFTObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    Py_ssize_t key_len = 0;
    const char* key = PyUnicode_AsUTF8AndSize(args[0], &key_len);
    if (!key) return NULL;
    MutableEntry* entry = mutable_lookup_entry(&self->table_state, key, key_len);
    if (entry) mutable_delete_entry_at(&self->table_state, entry);
    Py_RETURN_NONE;
}

static PyObject* native_mutable_clear_method(NativeMutableLQFTObject* self, PyObject* args) {
    mutable_clear_all(&self->table_state);
    Py_RETURN_NONE;
}

static PyObject* native_mutable_get_metrics_method(NativeMutableLQFTObject* self, PyObject* args) {
    return mutable_build_metrics(&self->table_state);
}

static PyObject* native_mutable_export_items_method(NativeMutableLQFTObject* self, PyObject* args) {
    return mutable_export_items_from_table(&self->table_state);
}

static Py_ssize_t native_mutable_len(NativeMutableLQFTObject* self) {
    return (Py_ssize_t)self->table_state.size;
}

static MutableEntry* mutable_lookup_entry(MutableTable* table_state, const char* key, Py_ssize_t key_len) {
    if (!table_state || !table_state->table || table_state->capacity == 0) return NULL;
    int found = 0;
    size_t idx = mutable_find_slot(table_state->table, table_state->capacity, key, key_len, hash_key_bytes(key, key_len), &found);
    return found ? &table_state->table[idx] : NULL;
}

static PyObject* method_mutable_new(PyObject* self, PyObject* args) {
    MutableTable* table_state = (MutableTable*)calloc(1, sizeof(MutableTable));
    if (!table_state) return PyErr_NoMemory();
    PyObject* capsule = PyCapsule_New(table_state, MUTABLE_TABLE_CAPSULE_NAME, mutable_table_capsule_destructor);
    if (!capsule) {
        free(table_state);
        return NULL;
    }
    return capsule;
}

static PyObject* method_mutable_insert_key_value(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 3) return NULL;
    MutableTable* table_state = mutable_table_from_capsule(args[0]);
    if (!table_state) return NULL;
    Py_ssize_t key_len = 0;
    const char* key = PyUnicode_AsUTF8AndSize(args[1], &key_len);
    if (!key) return NULL;
    if (!PyUnicode_Check(args[2])) {
        PyErr_Format(PyExc_TypeError, "LQFT values must be strings. Received: %s", Py_TYPE(args[2])->tp_name);
        return NULL;
    }
    if (mutable_insert_object(table_state, args[1], key, key_len, args[2]) != 0) return PyErr_NoMemory();
    Py_RETURN_NONE;
}

static PyObject* method_mutable_search_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    MutableTable* table_state = mutable_table_from_capsule(args[0]);
    if (!table_state) return NULL;
    Py_ssize_t key_len = 0;
    const char* key = PyUnicode_AsUTF8AndSize(args[1], &key_len);
    if (!key) return NULL;
    MutableEntry* entry = mutable_lookup_entry(table_state, key, key_len);
    if (!entry) Py_RETURN_NONE;
    return Py_NewRef(entry->value_obj);
}

static PyObject* method_mutable_contains_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    MutableTable* table_state = mutable_table_from_capsule(args[0]);
    if (!table_state) return NULL;
    Py_ssize_t key_len = 0;
    const char* key = PyUnicode_AsUTF8AndSize(args[1], &key_len);
    if (!key) return NULL;
    if (mutable_lookup_entry(table_state, key, key_len)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject* method_mutable_delete_key(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    MutableTable* table_state = mutable_table_from_capsule(args[0]);
    if (!table_state) return NULL;
    Py_ssize_t key_len = 0;
    const char* key = PyUnicode_AsUTF8AndSize(args[1], &key_len);
    if (!key) return NULL;
    MutableEntry* entry = mutable_lookup_entry(table_state, key, key_len);
    if (entry) {
        mutable_delete_entry_at(table_state, entry);
    }
    Py_RETURN_NONE;
}

static PyObject* method_mutable_clear(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    MutableTable* table_state = mutable_table_from_capsule(args[0]);
    if (!table_state) return NULL;
    mutable_clear_all(table_state);
    Py_RETURN_NONE;
}

static PyObject* method_mutable_len(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    MutableTable* table_state = mutable_table_from_capsule(args[0]);
    if (!table_state) return NULL;
    return PyLong_FromSize_t(table_state->size);
}

static PyObject* method_mutable_get_metrics(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    MutableTable* table_state = mutable_table_from_capsule(args[0]);
    if (!table_state) return NULL;
    return mutable_build_metrics(table_state);
}

static PyObject* method_mutable_export_items(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    MutableTable* table_state = mutable_table_from_capsule(args[0]);
    if (!table_state) return NULL;
    return mutable_export_items_from_table(table_state);
}

static PyMethodDef NativeMutableLQFTMethods[] = {
    {"insert", (PyCFunction)native_mutable_insert, METH_FASTCALL, "Insert key/value into native mutable hash table"},
    {"search", (PyCFunction)native_mutable_search, METH_FASTCALL, "Search key in native mutable hash table"},
    {"contains", (PyCFunction)native_mutable_contains, METH_FASTCALL, "Contains check in native mutable hash table"},
    {"remove", (PyCFunction)native_mutable_delete, METH_FASTCALL, "Delete key from native mutable hash table"},
    {"delete", (PyCFunction)native_mutable_delete, METH_FASTCALL, "Delete key from native mutable hash table"},
    {"clear", (PyCFunction)native_mutable_clear_method, METH_NOARGS, "Clear native mutable hash table"},
    {"get_metrics", (PyCFunction)native_mutable_get_metrics_method, METH_NOARGS, "Metrics for native mutable hash table"},
    {"export_items", (PyCFunction)native_mutable_export_items_method, METH_NOARGS, "Export native mutable hash table items"},
    {NULL, NULL, 0, NULL}
};

static PySequenceMethods NativeMutableLQFTSequenceMethods = {
    (lenfunc)native_mutable_len,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
};

static PyTypeObject NativeMutableLQFTType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "lqft_c_engine.NativeMutableLQFT",
    .tp_basicsize = sizeof(NativeMutableLQFTObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)native_mutable_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = "Native mutable LQFT hash table",
    .tp_methods = NativeMutableLQFTMethods,
    .tp_as_sequence = &NativeMutableLQFTSequenceMethods,
    .tp_init = (initproc)native_mutable_init,
    .tp_new = native_mutable_new,
};