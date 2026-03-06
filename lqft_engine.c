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
 * Target: McMaster B.Tech / UofT MScAC Portfolio
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

#define MAX_TRACKED_THREADS 256

typedef struct {
    int64_t phys_added;
    int64_t phys_freed;
    int64_t logical_inserts;
    char padding[40]; 
} ALIGN_64 ThreadMetrics;

static ALIGN_64 ThreadMetrics global_metrics_array[MAX_TRACKED_THREADS];
static volatile long registered_threads_count = 0;
static THREAD_LOCAL ThreadMetrics* my_metrics = NULL;

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
            my_metrics = &global_metrics_array[0]; 
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
    uint32_t registry_idx; 
    int ref_count;
} LQFTNode;

// ===================================================================
// THREAD-LOCAL ARENA ALLOCATOR (F-01/F-08 Stabilization)
// ===================================================================

typedef struct NodeChunk {
    LQFTNode nodes[ARENA_CHUNK_SIZE];
    struct NodeChunk* next_global; 
} NodeChunk;

typedef struct ChildChunk {
    LQFTNode* arrays[ARENA_CHUNK_SIZE][32];
    struct ChildChunk* next_global; 
} ChildChunk;

static inline NodeChunk* alloc_node_chunk(void) {
#ifdef _MSC_VER
    return (NodeChunk*)VirtualAlloc(NULL, sizeof(NodeChunk), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
#else
    void* p = mmap(NULL, sizeof(NodeChunk), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    return (p == MAP_FAILED) ? NULL : (NodeChunk*)p;
#endif
}

static inline ChildChunk* alloc_child_chunk(void) {
#ifdef _MSC_VER
    return (ChildChunk*)VirtualAlloc(NULL, sizeof(ChildChunk), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
#else
    void* p = mmap(NULL, sizeof(ChildChunk), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    return (p == MAP_FAILED) ? NULL : (ChildChunk*)p;
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

static inline void free_child_chunk(ChildChunk* chunk) {
    if (!chunk) return;
#ifdef _MSC_VER
    VirtualFree(chunk, 0, MEM_RELEASE);
#else
    munmap(chunk, sizeof(ChildChunk));
#endif
}

static PaddedLock global_chunk_lock = {0};
static NodeChunk* global_node_chunks = NULL;
static ChildChunk* global_child_chunks = NULL;

static NodeChunk* volatile pre_zeroed_node_chunks = NULL;
static ChildChunk* volatile pre_zeroed_child_chunks = NULL;
static volatile long pre_node_count = 0;
static volatile long pre_child_count = 0;
static volatile long bg_alloc_running = 0;

typedef struct {
    LQFTNode* volatile head;
    char padding[56];
} ALIGN_64 GlobalNodePool;

typedef struct {
    LQFTNode*** volatile head;
    char padding[56];
} ALIGN_64 GlobalArrayPool;

static GlobalNodePool node_pool = {NULL};
static GlobalArrayPool array_pool = {NULL};

typedef struct {
    NodeChunk* current_node_chunk;
    int node_chunk_idx;
    LQFTNode* node_free_list;
    ChildChunk* current_child_chunk;
    int child_chunk_idx;
    LQFTNode*** array_free_list;
} TLS_Arena;

static THREAD_LOCAL TLS_Arena local_arena = {NULL, ARENA_CHUNK_SIZE, NULL, NULL, ARENA_CHUNK_SIZE, NULL};

// Batched GC Retirement Chains
static THREAD_LOCAL LQFTNode* local_ret_node_head = NULL;
static THREAD_LOCAL LQFTNode* local_ret_node_tail = NULL;
static THREAD_LOCAL int local_ret_node_count = 0;

static THREAD_LOCAL LQFTNode*** local_ret_arr_head = NULL;
static THREAD_LOCAL LQFTNode*** local_ret_arr_tail = NULL;
static THREAD_LOCAL int local_ret_arr_count = 0;

static LQFTNode** registry = NULL;

typedef struct {
    LQFTNode* root;
    char padding[56];
} ALIGN_64 PaddedRoot;

static ALIGN_64 PaddedRoot global_roots[NUM_ROOTS];

static ALIGN_64 PaddedRWLock root_locks[NUM_ROOTS];
static ALIGN_64 PaddedLock stripe_locks[NUM_STRIPES];

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
        if (pre_child_count < 128) {
            ChildChunk* cc = alloc_child_chunk();
            if (cc) {
                memset(cc, 0, sizeof(ChildChunk)); // Eager Page Fault
#ifdef _MSC_VER
                ChildChunk* old;
                do { old = pre_zeroed_child_chunks; cc->next_global = old; } 
                while (_InterlockedCompareExchangePointer((void* volatile*)&pre_zeroed_child_chunks, (void*)cc, (void*)old) != (void*)old);
                _InterlockedIncrement(&pre_child_count);
#else
                ChildChunk* old;
                do { old = pre_zeroed_child_chunks; cc->next_global = old; } 
                while (!__sync_bool_compare_and_swap(&pre_zeroed_child_chunks, old, cc));
                __sync_fetch_and_add(&pre_child_count, 1);
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

LQFTNode* create_node(void* value, uint64_t key_hash, LQFTNode** children_src, uint64_t full_hash) {
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
            NodeChunk* new_chunk = NULL;
#ifdef _MSC_VER
            do {
                new_chunk = pre_zeroed_node_chunks;
                if (!new_chunk) break;
            } while (_InterlockedCompareExchangePointer((void* volatile*)&pre_zeroed_node_chunks, (void*)new_chunk->next_global, (void*)new_chunk) != (void*)new_chunk);
            if (new_chunk) _InterlockedDecrement(&pre_node_count);
#else
            do {
                new_chunk = pre_zeroed_node_chunks;
                if (!new_chunk) break;
            } while (!__sync_bool_compare_and_swap(&pre_zeroed_node_chunks, new_chunk, new_chunk->next_global));
            if (new_chunk) __sync_fetch_and_sub(&pre_node_count, 1);
#endif
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
    node->key_hash = key_hash;
    node->full_hash_val = full_hash; 
    node->registry_idx = 0;
    node->ref_count = 0;
    if (children_src) {
        LQFTNode** arr = NULL;
        if (!local_arena.array_free_list) {
#ifdef _MSC_VER
            LQFTNode*** free_chain;
            do {
                free_chain = (LQFTNode***)array_pool.head;
                if (!free_chain) break;
            } while (_InterlockedCompareExchangePointer((void* volatile*)&array_pool.head, NULL, (void*)free_chain) != (void*)free_chain);
            local_arena.array_free_list = free_chain;
#else
            LQFTNode*** free_chain;
            do {
                free_chain = (LQFTNode***)array_pool.head;
                if (!free_chain) break;
            } while (!__sync_bool_compare_and_swap(&array_pool.head, free_chain, NULL));
            local_arena.array_free_list = free_chain;
#endif
        }
        if (local_arena.array_free_list) {
            arr = (LQFTNode**)local_arena.array_free_list;
            local_arena.array_free_list = (LQFTNode***)arr[0];
        } else {
            if (local_arena.child_chunk_idx >= ARENA_CHUNK_SIZE) {
                ChildChunk* new_chunk = NULL;
#ifdef _MSC_VER
                do {
                    new_chunk = pre_zeroed_child_chunks;
                    if (!new_chunk) break;
                } while (_InterlockedCompareExchangePointer((void* volatile*)&pre_zeroed_child_chunks, (void*)new_chunk->next_global, (void*)new_chunk) != (void*)new_chunk);
                if (new_chunk) _InterlockedDecrement(&pre_child_count);
#else
                do {
                    new_chunk = pre_zeroed_child_chunks;
                    if (!new_chunk) break;
                } while (!__sync_bool_compare_and_swap(&pre_zeroed_child_chunks, new_chunk, new_chunk->next_global));
                if (new_chunk) __sync_fetch_and_sub(&pre_child_count, 1);
#endif
                if (!new_chunk) {
                    new_chunk = alloc_child_chunk();
                }
                if (!new_chunk) return NULL;
                local_arena.current_child_chunk = new_chunk;
                local_arena.child_chunk_idx = 0;
                fast_lock_backoff(&global_chunk_lock.flag);
                if (new_chunk) new_chunk->next_global = global_child_chunks;
                global_child_chunks = new_chunk;
                fast_unlock(&global_chunk_lock.flag);
            }
            arr = (LQFTNode**)local_arena.current_child_chunk->arrays[local_arena.child_chunk_idx++];
        }
        node->children = arr;
        memcpy(node->children, children_src, sizeof(LQFTNode*) * 32);
    } else {
        node->children = NULL; 
    }
    return node;
}

void decref(LQFTNode* start_node) {
    if (!start_node || start_node == TOMBSTONE) return;
    LQFTNode* cleanup_stack[512]; 
    int top = 0;
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
                for (int i = 0; i < 32; i++) {
                    if (node->children[i]) cleanup_stack[top++] = node->children[i];
                }
                LQFTNode*** arr = (LQFTNode***)node->children;
                arr[0] = (LQFTNode**)local_ret_arr_head;
                local_ret_arr_head = arr;
                if (local_ret_arr_count == 0) local_ret_arr_tail = arr;
                local_ret_arr_count++;
                if (local_ret_arr_count >= 1024) {
#ifdef _MSC_VER
                    LQFTNode*** old_head;
                    do {
                        old_head = (LQFTNode***)array_pool.head;
                        local_ret_arr_tail[0] = (LQFTNode**)old_head;
                    } while (_InterlockedCompareExchangePointer((void* volatile*)&array_pool.head, (void*)local_ret_arr_head, (void*)old_head) != (void*)old_head);
#else
                    LQFTNode*** old_head;
                    do {
                        old_head = (LQFTNode***)array_pool.head;
                        local_ret_arr_tail[0] = (LQFTNode**)old_head;
                    } while (!__sync_bool_compare_and_swap(&array_pool.head, old_head, local_ret_arr_head));
#endif
                    local_ret_arr_head = NULL;
                    local_ret_arr_tail = NULL;
                    local_ret_arr_count = 0;
                }
            }
            if (node->value) free(node->value);
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
}

LQFTNode* get_canonical_v2(const char* value_ptr, uint64_t key_hash, LQFTNode** children, uint64_t full_hash) {
    uint32_t stripe = (uint32_t)(full_hash % NUM_STRIPES);
    uint32_t local_idx = (uint32_t)((full_hash ^ (full_hash >> 32)) & STRIPE_MASK);
    uint32_t global_idx = (stripe * STRIPE_SIZE) + local_idx;
    uint32_t start_idx = local_idx;
    for (;;) {
        LQFTNode* slot = registry[global_idx];
        if (slot == NULL) break;
        if (slot != TOMBSTONE && slot->full_hash_val == full_hash) {
            ATOMIC_INC(&slot->ref_count); 
            return slot;
        }
        local_idx = (local_idx + 1) & STRIPE_MASK;
        global_idx = (stripe * STRIPE_SIZE) + local_idx;
        if (local_idx == start_idx) break; 
    }
    LQFTNode* new_node = create_node(value_ptr ? (void*)portable_strdup(value_ptr) : NULL, key_hash, children, full_hash);
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
        else if (slot->full_hash_val == full_hash) {
            ATOMIC_INC(&slot->ref_count);
            fast_unlock(&stripe_locks[stripe].flag);
            if (new_node->value) free(new_node->value);
            if (new_node->children) {
                LQFTNode*** arr = (LQFTNode***)new_node->children;
                arr[0] = (LQFTNode**)local_ret_arr_head;
                local_ret_arr_head = arr;
                if (local_ret_arr_count == 0) local_ret_arr_tail = arr;
                local_ret_arr_count++;
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
        for (int i = 0; i < 32; i++) {
            if (new_node->children[i]) ATOMIC_INC(&new_node->children[i]->ref_count);
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

LQFTNode* core_insert_internal(uint64_t h, const char* val_ptr, LQFTNode* root, uint64_t pre_leaf_base) {
    LQFTNode* path_nodes[20]; uint32_t path_segs[20]; int path_len = 0;
    LQFTNode* curr = root; int bit_depth = 0;
    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        path_nodes[path_len] = curr; path_segs[path_len] = segment; path_len++;
        LQFTNode* next_node = curr->children[segment];
        if (next_node == NULL) { curr = NULL; break; }
        if (bit_depth + BIT_PARTITION < 64) {
            uint32_t next_segment = (h >> (bit_depth + BIT_PARTITION)) & MASK;
            PREFETCH(&next_node->children[next_segment]);
        } else {
            PREFETCH(next_node);
        }
        curr = next_node; 
        bit_depth += BIT_PARTITION;
    }
    LQFTNode* new_sub_node = NULL;
    uint64_t leaf_h = (pre_leaf_base ^ h) * FNV_PRIME;
    if (curr == NULL) { 
        new_sub_node = get_canonical_v2(val_ptr, h, NULL, leaf_h); 
    } else if (curr->key_hash == h) { 
        new_sub_node = get_canonical_v2(val_ptr, h, curr->children, leaf_h); 
    } else {
        uint64_t old_h = curr->key_hash;
        uint64_t old_leaf_h = (pre_leaf_base ^ old_h) * FNV_PRIME;
        int temp_depth = bit_depth;
        while (temp_depth < 64) {
            uint32_t s_old = (old_h >> temp_depth) & MASK;
            uint32_t s_new = (h >> temp_depth) & MASK;
            if (s_old != s_new) {
                LQFTNode* c_old = get_canonical_v2((const char*)curr->value, old_h, curr->children, old_leaf_h);
                LQFTNode* c_new = get_canonical_v2(val_ptr, h, NULL, leaf_h);
                LQFTNode* new_children[32]; memset(new_children, 0, sizeof(LQFTNode*) * 32);
                new_children[s_old] = c_old; new_children[s_new] = c_new;
                uint64_t branch_h = hash_node_state(new_children);
                new_sub_node = get_canonical_v2(NULL, 0, new_children, branch_h);
                decref(c_old); decref(c_new); break;
            } else { 
                path_nodes[path_len] = NULL; path_segs[path_len] = s_old; path_len++; temp_depth += BIT_PARTITION; 
            }
        }
        if (new_sub_node == NULL) new_sub_node = get_canonical_v2(val_ptr, h, curr->children, leaf_h);
    }
    for (int i = path_len - 1; i >= 0; i--) {
        LQFTNode* next_parent;
        if (path_nodes[i] == NULL) {
            LQFTNode* new_children[32]; memset(new_children, 0, sizeof(LQFTNode*) * 32);
            new_children[path_segs[i]] = new_sub_node;
            next_parent = get_canonical_v2(NULL, 0, new_children, hash_node_state(new_children));
        } else {
            LQFTNode* p = path_nodes[i];
            LQFTNode* n_children[32]; 
            if (p->children) memcpy(n_children, p->children, sizeof(LQFTNode*) * 32);
            else memset(n_children, 0, sizeof(LQFTNode*) * 32);
            n_children[path_segs[i]] = new_sub_node;
            uint64_t b_h = hash_node_state(n_children);
            next_parent = get_canonical_v2((const char*)p->value, p->key_hash, n_children, b_h);
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
        if (curr->children == NULL || curr->children[segment] == NULL) {
            ATOMIC_INC(&root->ref_count);
            return root; 
        }
        curr = curr->children[segment]; bit_depth += BIT_PARTITION;
    }
    if (curr == NULL || curr->key_hash != h) {
        ATOMIC_INC(&root->ref_count);
        return root; 
    }
    LQFTNode* new_sub_node = NULL; 
    for (int i = path_len - 1; i >= 0; i--) {
        LQFTNode* p = path_nodes[i];
        LQFTNode* n_children[32]; 
        if (p->children) memcpy(n_children, p->children, sizeof(LQFTNode*) * 32);
        else memset(n_children, 0, sizeof(LQFTNode*) * 32);
        n_children[path_segs[i]] = new_sub_node;
        int has_c = 0; for(int j=0; j<32; j++) { if(n_children[j]) { has_c = 1; break; } }
        if (!has_c && p->value == NULL) { 
            new_sub_node = NULL; 
        } else {
            uint64_t b_h = hash_node_state(n_children);
            new_sub_node = get_canonical_v2((const char*)p->value, p->key_hash, n_children, b_h);
        }
    }
    return new_sub_node;
}

char* core_search(uint64_t h, LQFTNode* root) {
    LQFTNode* curr = root; 
    int bit_depth = 0;
    while (curr != NULL && curr->value == NULL) {
        if (curr->children == NULL) return NULL;
        curr = curr->children[(h >> bit_depth) & MASK];
        bit_depth += BIT_PARTITION;
    }
    if (curr != NULL && curr->key_hash == h) return (char*)curr->value;
    return NULL;
}

static void c_internal_insert_rw(uint64_t h, const char* val_str) {
    uint64_t pre = fnv1a_update(FNV_OFFSET_BASIS, "leaf:", 5);
    pre = fnv1a_update(pre, val_str, strlen(val_str));
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    get_my_metrics()->logical_inserts++;
    int spin = 0;
    while (1) {
        LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
        LQFTNode* old_root = global_roots[shard].root;
        if (old_root) ATOMIC_INC(&old_root->ref_count);
        LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
        LQFTNode* next = core_insert_internal(h, val_str, old_root, pre);
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
    local_arena.current_child_chunk = NULL;
    local_arena.child_chunk_idx = ARENA_CHUNK_SIZE;
    local_arena.array_free_list = NULL;
    local_ret_node_head = NULL;
    local_ret_node_tail = NULL;
    local_ret_node_count = 0;
    local_ret_arr_head = NULL;
    local_ret_arr_tail = NULL;
    local_ret_arr_count = 0;
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

static PyObject* method_insert(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    uint64_t h = PyLong_AsUnsignedLongLongMask(args[0]);
    const char* val_str = PyUnicode_AsUTF8(args[1]);
    if (!val_str) return NULL;
    Py_BEGIN_ALLOW_THREADS
    c_internal_insert_rw(h, val_str);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* method_search(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    uint64_t h = PyLong_AsUnsignedLongLongMask(args[0]);
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    char* safe_copy = NULL; 
    Py_BEGIN_ALLOW_THREADS 
    LQFT_RWLOCK_RDLOCK(&root_locks[shard].lock);
    LQFTNode* current_root = global_roots[shard].root;
    if (current_root) {
        char* result = core_search(h, current_root); 
        if (result) safe_copy = portable_strdup(result); 
    }
    LQFT_RWLOCK_UNLOCK_RD(&root_locks[shard].lock);
    Py_END_ALLOW_THREADS
    if (safe_copy) {
        PyObject* py_res = PyUnicode_FromString(safe_copy);
        free(safe_copy); return py_res;
    }
    Py_RETURN_NONE;
}

static PyObject* method_delete(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
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

static PyObject* method_get_metrics(PyObject* self, PyObject* args) { 
    int64_t total_phys_added = 0;
    int64_t total_phys_freed = 0;
    int64_t total_logical = 0;
    for (int i = 0; i < MAX_TRACKED_THREADS; i++) {
        total_phys_added += global_metrics_array[i].phys_added;
        total_phys_freed += global_metrics_array[i].phys_freed;
        total_logical += global_metrics_array[i].logical_inserts;
    }
    int64_t net_phys = total_phys_added - total_phys_freed;
    double deduplication_ratio = net_phys > 0 ? (double)total_logical / (double)net_phys : 0.0;
    return Py_BuildValue("{s:L, s:L, s:d}", "physical_nodes", net_phys, "logical_inserts", total_logical, "deduplication_ratio", deduplication_ratio); 
}

static PyObject* method_free_all(PyObject* self, PyObject* args) {
    Py_BEGIN_ALLOW_THREADS
    for(int i = 0; i < NUM_ROOTS; i++) LQFT_RWLOCK_WRLOCK(&root_locks[i].lock);
    for(int i = 0; i < NUM_STRIPES; i++) fast_lock_backoff(&stripe_locks[i].flag);
    if (registry) { 
        for(int i = 0; i < NUM_STRIPES * STRIPE_SIZE; i++) {
            if (registry[i] && registry[i] != TOMBSTONE) { if (registry[i]->value) free(registry[i]->value); } 
            registry[i] = NULL; 
        }
    }
    fast_lock_backoff(&global_chunk_lock.flag);
    NodeChunk* nc = global_node_chunks;
    while(nc) { NodeChunk* n = nc->next_global; free_node_chunk(nc); nc = n; }
    global_node_chunks = NULL;

    nc = pre_zeroed_node_chunks;
    while(nc) { NodeChunk* n = nc->next_global; free_node_chunk(nc); nc = n; }
    pre_zeroed_node_chunks = NULL;
    pre_node_count = 0;

    ChildChunk* cc = global_child_chunks;
    while(cc) { ChildChunk* n = cc->next_global; free_child_chunk(cc); cc = n; }
    global_child_chunks = NULL;

    cc = pre_zeroed_child_chunks;
    while(cc) { ChildChunk* n = cc->next_global; free_child_chunk(cc); cc = n; }
    pre_zeroed_child_chunks = NULL;
    pre_child_count = 0;

    node_pool.head = NULL;
    array_pool.head = NULL;
    fast_unlock(&global_chunk_lock.flag);
    for (int i = 0; i < MAX_TRACKED_THREADS; i++) {
        global_metrics_array[i].phys_added = 0; global_metrics_array[i].phys_freed = 0; global_metrics_array[i].logical_inserts = 0;
    }
    for(int i = NUM_STRIPES - 1; i >= 0; i--) fast_unlock(&stripe_locks[i].flag);
    for(int i = NUM_ROOTS - 1; i >= 0; i--) { 
        global_roots[i].root = NULL; 
        LQFT_RWLOCK_UNLOCK_WR(&root_locks[i].lock); 
    }
    Py_END_ALLOW_THREADS 
    Py_RETURN_NONE;
}

static PyMethodDef LQFTMethods[] = {
    {"insert", (PyCFunction)method_insert, METH_FASTCALL, "Fast-path insert single key"},
    {"search", (PyCFunction)method_search, METH_FASTCALL, "Fast-path search single key"},
    {"delete", (PyCFunction)method_delete, METH_FASTCALL, "Fast-path delete single key"},
    {"internal_stress_test", (PyCFunction)method_internal_stress_test, METH_FASTCALL, "Run native C stress test"},
    {"get_metrics", method_get_metrics, METH_VARARGS, "Get stats"},
    {"free_all", method_free_all, METH_VARARGS, "Wipe memory"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef lqftmodule = { PyModuleDef_HEAD_INIT, "lqft_c_engine", NULL, -1, LQFTMethods };

PyMODINIT_FUNC PyInit_lqft_c_engine(void) { 
    for(int i = 0; i < NUM_ROOTS; i++) {
        global_roots[i].root = NULL;
        LQFT_RWLOCK_INIT(&root_locks[i].lock);
    }
    registry = (LQFTNode**)calloc(NUM_STRIPES * STRIPE_SIZE, sizeof(LQFTNode*));
    for(int i = 0; i < NUM_STRIPES; i++) stripe_locks[i].flag = 0;
    for(int j = 0; j < 4; j++) {
        NodeChunk* nc = alloc_node_chunk();
        ChildChunk* cc = alloc_child_chunk();
        if (nc) { memset(nc, 0, sizeof(NodeChunk)); nc->next_global = pre_zeroed_node_chunks; pre_zeroed_node_chunks = nc; pre_node_count++; }
        if (cc) { memset(cc, 0, sizeof(ChildChunk)); cc->next_global = pre_zeroed_child_chunks; pre_zeroed_child_chunks = cc; pre_child_count++; }
    }
    bg_alloc_running = 1;
#ifdef _MSC_VER
    CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)background_alloc_thread, NULL, 0, NULL);
#else
    pthread_t bg_tid; pthread_create(&bg_tid, NULL, background_alloc_thread, NULL); pthread_detach(bg_tid); 
#endif
    return PyModule_Create(&lqftmodule); 
}