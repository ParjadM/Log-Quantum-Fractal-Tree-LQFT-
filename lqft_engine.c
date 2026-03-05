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
 * LQFT C-Engine - V1.0.7 (Gold Master Edition)
 * Architect: Parjad Minooei
 * Target: McMaster B.Tech Portfolio
 * * SYSTEMS ARCHITECTURE MILESTONES:
 * 1. DAEMON ALLOCATOR: A background thread pre-allocates zeroed memory chunks 
 * to prevent worker threads from stalling on OS mmap faults during 1B-node runs.
 * 2. GRANDCHILD PREFETCHING: Look-ahead caching masks DDR4/DDR5 latency.
 * 3. METRIC SHARDING: Zero-contention padded thread-local metric arrays.
 * 4. STRICT CACHE ALIGNMENT: Lock arrays padded to stop False Sharing.
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
#else
    #include <pthread.h>
    #include <sched.h>
    #include <sys/mman.h>
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
#endif

// ===================================================================
// CACHE-ALIGNED INFRASTRUCTURE & LOCKS
// ===================================================================

typedef struct {
    volatile long flag;
    char padding[60]; 
} ALIGN_64 PaddedLock;

static inline void fast_lock_backoff(volatile long* lk) {
    int spin = 0;
#ifdef _MSC_VER
    while (_InterlockedCompareExchange(lk, 1, 0) == 1) {
#else
    while (__sync_val_compare_and_swap(lk, 0, 1) == 1) {
#endif
        spin++;
        int max_spin = 1 << (spin < 12 ? spin : 12);
        for(volatile int s = 0; s < max_spin; s++) { CPU_PAUSE; }
        
        if (spin > 10) {
#ifdef _MSC_VER
            SwitchToThread();
#else
            sched_yield();
#endif
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
// THREAD-LOCAL METRIC SHARDING (Zero Contention)
// ===================================================================

#define MAX_TRACKED_THREADS 256

typedef struct {
    int64_t phys_added;
    int64_t phys_freed;
    int64_t logical_inserts;
    char padding[40]; // 64 - 24 = 40 bytes padding
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
#define NUM_STRIPES 2048
#define STRIPE_SIZE 16384 
#define STRIPE_MASK (STRIPE_SIZE - 1)
#define TOMBSTONE ((LQFTNode*)1)

#define NUM_ROOTS 2048
#define ROOT_MASK 0x7FF
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
// BACKGROUND DAEMON ALLOCATOR & MEMORY ARENAS
// ===================================================================

typedef struct NodeChunk {
    LQFTNode nodes[ARENA_CHUNK_SIZE];
    struct NodeChunk* next_global; 
} NodeChunk;

typedef struct ChildChunk {
    LQFTNode* arrays[ARENA_CHUNK_SIZE][32];
    struct ChildChunk* next_global; 
} ChildChunk;

static PaddedLock global_chunk_lock = {0};
static NodeChunk* global_node_chunks = NULL;
static ChildChunk* global_child_chunks = NULL;

// Background Pre-Zeroed Queues
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

// V1.0.7 FIX: Restored global roots array missing in previous copy
static LQFTNode* global_roots[NUM_ROOTS];
static ALIGN_64 PaddedLock root_locks[NUM_ROOTS];
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
    uint64_t hval = FNV_OFFSET_BASIS;
    if (children) {
        for (int i = 0; i < 32; i++) {
            uint64_t c_hash = children[i] ? children[i]->full_hash_val : 0;
            hval ^= c_hash;
            hval *= FNV_PRIME;
        }
    }
    return hval;
}

char* portable_strdup(const char* s) {
    if (!s) return NULL;
#ifdef _WIN32
    return _strdup(s);
#else
    return strdup(s);
#endif
}

// Background Daemon Worker
#ifdef _MSC_VER
DWORD WINAPI background_alloc_thread(LPVOID arg) {
#else
void* background_alloc_thread(void* arg) {
#endif
    while(bg_alloc_running) {
        int work_done = 0;
        
        // Maintain a buffer of 32 chunks (~25MB pre-allocated per type)
        if (pre_node_count < 32) {
#ifdef _MSC_VER
            NodeChunk* nc = (NodeChunk*)VirtualAlloc(NULL, sizeof(NodeChunk), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
#else
            NodeChunk* nc = (NodeChunk*)mmap(NULL, sizeof(NodeChunk), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
#endif
            if (nc) {
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
        
        if (pre_child_count < 32) {
#ifdef _MSC_VER
            ChildChunk* cc = (ChildChunk*)VirtualAlloc(NULL, sizeof(ChildChunk), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
#else
            ChildChunk* cc = (ChildChunk*)mmap(NULL, sizeof(ChildChunk), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
#endif
            if (cc) {
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
            free_chain = node_pool.head;
            if (!free_chain) break;
        } while (_InterlockedCompareExchangePointer((void* volatile*)&node_pool.head, NULL, (void*)free_chain) != (void*)free_chain);
        local_arena.node_free_list = free_chain;
#else
        LQFTNode* free_chain;
        do {
            free_chain = node_pool.head;
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
            
            // Pop from Daemon queue to prevent OS mmap stall
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

            // Fallback if Daemon is behind
            if (!new_chunk) {
#ifdef _MSC_VER
                new_chunk = (NodeChunk*)VirtualAlloc(NULL, sizeof(NodeChunk), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
#else
                new_chunk = (NodeChunk*)mmap(NULL, sizeof(NodeChunk), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
#endif
            }
            if (!new_chunk) {
                printf("[!] FATAL ERROR: OS Virtual Memory Exhausted (OOM).\n");
                exit(1);
            }
            local_arena.current_node_chunk = new_chunk;
            local_arena.node_chunk_idx = 0;
            
            fast_lock_backoff(&global_chunk_lock.flag);
            new_chunk->next_global = global_node_chunks;
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
        LQFTNode*** arr = NULL;

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
                free_chain = array_pool.head;
                if (!free_chain) break;
            } while (!__sync_bool_compare_and_swap(&array_pool.head, free_chain, NULL));
            local_arena.array_free_list = free_chain;
#endif
        }

        if (local_arena.array_free_list) {
            arr = local_arena.array_free_list;
            local_arena.array_free_list = (LQFTNode***)arr[0];
        } else {
            if (local_arena.child_chunk_idx >= ARENA_CHUNK_SIZE) {
                ChildChunk* new_chunk = NULL;
                
                // Pop from Daemon
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
#ifdef _MSC_VER
                    new_chunk = (ChildChunk*)VirtualAlloc(NULL, sizeof(ChildChunk), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
#else
                    new_chunk = (ChildChunk*)mmap(NULL, sizeof(ChildChunk), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
#endif
                }
                if (!new_chunk) {
                    printf("[!] FATAL ERROR: OS Virtual Memory Exhausted (OOM).\n");
                    exit(1);
                }
                
                local_arena.current_child_chunk = new_chunk;
                local_arena.child_chunk_idx = 0;
                
                fast_lock_backoff(&global_chunk_lock.flag);
                new_chunk->next_global = global_child_chunks;
                global_child_chunks = new_chunk;
                fast_unlock(&global_chunk_lock.flag);
            }
            arr = local_arena.current_child_chunk->arrays[local_arena.child_chunk_idx++];
        }
        node->children = (LQFTNode**)arr;
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
                        old_head = array_pool.head;
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
                    old_node_head = node_pool.head;
                    local_ret_node_tail->children = (LQFTNode**)old_node_head;
                } while (_InterlockedCompareExchangePointer((void* volatile*)&node_pool.head, (void*)local_ret_node_head, (void*)old_node_head) != (void*)old_node_head);
#else
                LQFTNode* old_node_head;
                do {
                    old_node_head = node_pool.head;
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
    if (new_node->children) {
        for (int i = 0; i < 32; i++) {
            if (new_node->children[i]) ATOMIC_INC(&new_node->children[i]->ref_count);
        }
    }
    
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
            decref(new_node); 
            return slot;
        }
        local_idx = (local_idx + 1) & STRIPE_MASK;
        global_idx = (stripe * STRIPE_SIZE) + local_idx;
        if (local_idx == start_idx) break; 
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
        
        // V1.0.7 FIX: Grandchild Look-Ahead Prefetch
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

char* core_search(uint64_t h, LQFTNode* root) {
    LQFTNode* curr = root; 
    int bit_depth = 0;
    while (curr != NULL && curr->value == NULL) {
        if (curr->children == NULL) return NULL;
        uint32_t segment = (h >> bit_depth) & MASK;
        LQFTNode* next_node = curr->children[segment];
        
        if (next_node) {
            if (bit_depth + BIT_PARTITION < 64) {
                uint32_t next_segment = (h >> (bit_depth + BIT_PARTITION)) & MASK;
                PREFETCH(&next_node->children[next_segment]);
            } else {
                PREFETCH(next_node);
            }
        }
        
        curr = next_node;
        bit_depth += BIT_PARTITION;
    }
    if (curr != NULL && curr->key_hash == h) return (char*)curr->value;
    return NULL;
}

static void c_internal_insert(uint64_t h, const char* val_str) {
    uint64_t pre = fnv1a_update(FNV_OFFSET_BASIS, "leaf:", 5);
    pre = fnv1a_update(pre, val_str, strlen(val_str));
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    
    get_my_metrics()->logical_inserts++;

    int spin = 0;
    while (1) {
        fast_lock_backoff(&root_locks[shard].flag);
        LQFTNode* old_root = global_roots[shard];
        if (old_root) ATOMIC_INC(&old_root->ref_count);
        fast_unlock(&root_locks[shard].flag);

        LQFTNode* next = core_insert_internal(h, val_str, old_root, pre);

        fast_lock_backoff(&root_locks[shard].flag);
        if (global_roots[shard] == old_root) {
            global_roots[shard] = next;
            fast_unlock(&root_locks[shard].flag);
            if (old_root) { decref(old_root); decref(old_root); }
            break;
        } else {
            fast_unlock(&root_locks[shard].flag);
            if (next) decref(next);
            if (old_root) decref(old_root);
            spin++;
            int max_spin = 1 << (spin < 12 ? spin : 12);
            for(volatile int s = 0; s < max_spin; s++) { CPU_PAUSE; }
            if (spin > 10) {
#ifdef _MSC_VER
                SwitchToThread();
#else
                sched_yield();
#endif
            }
        }
    }
}

// ===================================================================
// V1.0.7 API BINDINGS
// ===================================================================

static PyObject* method_insert(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 2) return NULL;
    uint64_t h = PyLong_AsUnsignedLongLongMask(args[0]);
    const char* val_str = PyUnicode_AsUTF8(args[1]);
    if (!val_str) return NULL;

    Py_BEGIN_ALLOW_THREADS
    c_internal_insert(h, val_str);
    Py_END_ALLOW_THREADS
    
    Py_RETURN_NONE;
}

static PyObject* method_search(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 1) return NULL;
    uint64_t h = PyLong_AsUnsignedLongLongMask(args[0]);
    uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
    char* safe_copy = NULL; 
    
    Py_BEGIN_ALLOW_THREADS 
    fast_lock_backoff(&root_locks[shard].flag);
    LQFTNode* current_root = global_roots[shard];
    if (current_root) ATOMIC_INC(&current_root->ref_count);
    fast_unlock(&root_locks[shard].flag);
    
    if (current_root) {
        char* result = core_search(h, current_root); 
        if (result) safe_copy = portable_strdup(result); 
        decref(current_root);
    }
    Py_END_ALLOW_THREADS
    
    if (safe_copy) {
        PyObject* py_res = PyUnicode_FromString(safe_copy);
        free(safe_copy); return py_res;
    }
    Py_RETURN_NONE;
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
    
    // Explicitly grab a tracked metric slot
    my_metrics = NULL;
    get_my_metrics();

    StressArgs* sargs = (StressArgs*)arg;
    uint32_t rng_state = 123456789 ^ (sargs->thread_id * 1999999973);
    
    char val_buf[32] = "val";
    
    for (int i = 0; i < sargs->ops; i++) {
        uint32_t roll = xorshift32(&rng_state) % 100;
        uint64_t h = ((uint64_t)xorshift32(&rng_state) << 32) | xorshift32(&rng_state);
        
        if (roll < (uint32_t)sargs->write_threshold) {
            c_internal_insert(h, val_buf);
        } else {
            uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
            
            fast_lock_backoff(&root_locks[shard].flag);
            LQFTNode* current_root = global_roots[shard];
            if (current_root) ATOMIC_INC(&current_root->ref_count);
            fast_unlock(&root_locks[shard].flag);

            if (current_root) {
                core_search(h, current_root);
                decref(current_root);
            }
        }
    }
    
    if (local_ret_node_count > 0) {
#ifdef _MSC_VER
        LQFTNode* old_node_head;
        do {
            old_node_head = node_pool.head;
            local_ret_node_tail->children = (LQFTNode**)old_node_head;
        } while (_InterlockedCompareExchangePointer((void* volatile*)&node_pool.head, (void*)local_ret_node_head, (void*)old_node_head) != (void*)old_node_head);
#else
        LQFTNode* old_node_head;
        do {
            old_node_head = node_pool.head;
            local_ret_node_tail->children = (LQFTNode**)old_node_head;
        } while (!__sync_bool_compare_and_swap(&node_pool.head, old_node_head, local_ret_node_head));
#endif
        local_ret_node_head = NULL; local_ret_node_tail = NULL; local_ret_node_count = 0;
    }
    
    if (local_ret_arr_count > 0) {
#ifdef _MSC_VER
        LQFTNode*** old_head;
        do {
            old_head = (LQFTNode***)array_pool.head;
            local_ret_arr_tail[0] = (LQFTNode**)old_head;
        } while (_InterlockedCompareExchangePointer((void* volatile*)&array_pool.head, (void*)local_ret_arr_head, (void*)old_head) != (void*)old_head);
#else
        LQFTNode*** old_head;
        do {
            old_head = array_pool.head;
            local_ret_arr_tail[0] = (LQFTNode**)old_head;
        } while (!__sync_bool_compare_and_swap(&array_pool.head, old_head, local_ret_arr_head));
#endif
        local_ret_arr_head = NULL; local_ret_arr_tail = NULL; local_ret_arr_count = 0;
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
        threads[i] = CreateThread(NULL, 0, stress_worker, &t_args[i], 0, NULL);
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

static PyObject* method_insert_batch_raw(PyObject* self, PyObject* args) {
    Py_buffer buf; const char* val_ptr; 
    if (!PyArg_ParseTuple(args, "y*s", &buf, &val_ptr)) return NULL;
    
    Py_ssize_t len = buf.len / sizeof(uint64_t); 
    const uint64_t* hashes = (const uint64_t*)buf.buf;
    
    uint64_t pre = fnv1a_update(FNV_OFFSET_BASIS, "leaf:", 5);
    pre = fnv1a_update(pre, val_ptr, strlen(val_ptr));

    Py_BEGIN_ALLOW_THREADS
    for (Py_ssize_t i = 0; i < len; i++) {
        uint64_t h = hashes[i];
        uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
        
        int spin = 0;
        while(1) {
            fast_lock_backoff(&root_locks[shard].flag);
            LQFTNode* old_root = global_roots[shard];
            if (old_root) ATOMIC_INC(&old_root->ref_count);
            fast_unlock(&root_locks[shard].flag);

            LQFTNode* next = core_insert_internal(h, val_ptr, old_root, pre);

            fast_lock_backoff(&root_locks[shard].flag);
            if (global_roots[shard] == old_root) {
                global_roots[shard] = next;
                fast_unlock(&root_locks[shard].flag);
                if (old_root) { decref(old_root); decref(old_root); }
                break; 
            } else {
                fast_unlock(&root_locks[shard].flag);
                if (next) decref(next);
                if (old_root) decref(old_root);
                spin++;
                int max_spin = 1 << (spin < 12 ? spin : 12);
                for(volatile int s = 0; s < max_spin; s++) { CPU_PAUSE; }
                if (spin > 10) {
#ifdef _MSC_VER
                    SwitchToThread();
#else
                    sched_yield();
#endif
                }
            }
        }
    }
    
    get_my_metrics()->logical_inserts += len;
    
    Py_END_ALLOW_THREADS
    PyBuffer_Release(&buf); 
    Py_RETURN_NONE;
}

static PyObject* method_search_batch(PyObject* self, PyObject* args) {
    PyObject* py_list; if (!PyArg_ParseTuple(args, "O", &py_list)) return NULL;
    PyObject* seq = PySequence_Fast(py_list, "List expected."); if (!seq) return NULL;
    Py_ssize_t len = PySequence_Fast_GET_SIZE(seq); 
    uint64_t* hashes = (uint64_t*)malloc(len * sizeof(uint64_t));
    PyObject** items = PySequence_Fast_ITEMS(seq);
    for (Py_ssize_t i = 0; i < len; i++) hashes[i] = PyLong_AsUnsignedLongLongMask(items[i]);
    Py_DECREF(seq); 
    int hits = 0;
    
    Py_BEGIN_ALLOW_THREADS 
    for (Py_ssize_t i = 0; i < len; i++) {
        uint64_t h = hashes[i];
        uint32_t shard = (uint32_t)((h >> 48) & ROOT_MASK);
        
        fast_lock_backoff(&root_locks[shard].flag);
        LQFTNode* current_root = global_roots[shard];
        if (current_root) ATOMIC_INC(&current_root->ref_count);
        fast_unlock(&root_locks[shard].flag);
        
        if (current_root) {
            if (core_search(h, current_root) != NULL) hits++;
            decref(current_root);
        }
    }
    Py_END_ALLOW_THREADS
    free(hashes); return PyLong_FromLong(hits);
}

static PyObject* method_get_metrics(PyObject* self, PyObject* args) { 
    int64_t total_phys_added = 0;
    int64_t total_phys_freed = 0;
    int64_t total_logical = 0;
    
    // V1.0.7 FIX: Safely aggregate sharded metrics with zero contention
    for (int i = 0; i < MAX_TRACKED_THREADS; i++) {
        total_phys_added += global_metrics_array[i].phys_added;
        total_phys_freed += global_metrics_array[i].phys_freed;
        total_logical += global_metrics_array[i].logical_inserts;
    }
    
    int64_t net_phys = total_phys_added - total_phys_freed;
    double deduplication_ratio = 0.0;
    
    if (net_phys > 0) {
        deduplication_ratio = (double)total_logical / (double)net_phys;
    }
    
    return Py_BuildValue("{s:L, s:L, s:d}", 
        "physical_nodes", net_phys,
        "logical_inserts", total_logical,
        "deduplication_ratio", deduplication_ratio); 
}

static PyObject* method_free_all(PyObject* self, PyObject* args) {
    Py_BEGIN_ALLOW_THREADS
    for(int i = 0; i < NUM_ROOTS; i++) fast_lock_backoff(&root_locks[i].flag);
    for(int i = 0; i < NUM_STRIPES; i++) fast_lock_backoff(&stripe_locks[i].flag);
    
    if (registry) { 
        for(int i = 0; i < NUM_STRIPES * STRIPE_SIZE; i++) {
            if (registry[i] && registry[i] != TOMBSTONE) { 
                if (registry[i]->value) free(registry[i]->value);
            } 
            registry[i] = NULL; 
        }
    }
    
    fast_lock_backoff(&global_chunk_lock.flag);
    
    // Clear Pre-Zeroed Daemon Queues safely
    NodeChunk* pnc = pre_zeroed_node_chunks;
    while(pnc) {
        NodeChunk* next = pnc->next_global;
#ifdef _MSC_VER
        VirtualFree(pnc, 0, MEM_RELEASE); 
#else
        munmap(pnc, sizeof(NodeChunk));
#endif
        pnc = next;
    }
    pre_zeroed_node_chunks = NULL;
    pre_node_count = 0;
    
    ChildChunk* pcc = pre_zeroed_child_chunks;
    while(pcc) {
        ChildChunk* next = pcc->next_global;
#ifdef _MSC_VER
        VirtualFree(pcc, 0, MEM_RELEASE); 
#else
        munmap(pcc, sizeof(ChildChunk));
#endif
        pcc = next;
    }
    pre_zeroed_child_chunks = NULL;
    pre_child_count = 0;
    
    // Clear Standard Active Queues
    NodeChunk* nc = global_node_chunks;
    while(nc) { 
        NodeChunk* next = nc->next_global; 
#ifdef _MSC_VER
        VirtualFree(nc, 0, MEM_RELEASE); 
#else
        munmap(nc, sizeof(NodeChunk));
#endif
        nc = next; 
    }
    global_node_chunks = NULL;

    ChildChunk* cc = global_child_chunks;
    while(cc) { 
        ChildChunk* next = cc->next_global; 
#ifdef _MSC_VER
        VirtualFree(cc, 0, MEM_RELEASE); 
#else
        munmap(cc, sizeof(ChildChunk));
#endif
        cc = next; 
    }
    global_child_chunks = NULL;
    
    node_pool.head = NULL;
    array_pool.head = NULL;
    fast_unlock(&global_chunk_lock.flag);

    // Reset TLS
    local_arena.current_node_chunk = NULL;
    local_arena.node_chunk_idx = ARENA_CHUNK_SIZE;
    local_arena.node_free_list = NULL;
    local_arena.current_child_chunk = NULL;
    local_arena.child_chunk_idx = ARENA_CHUNK_SIZE;
    local_arena.array_free_list = NULL;
    my_metrics = NULL;

    for (int i = 0; i < MAX_TRACKED_THREADS; i++) {
        global_metrics_array[i].phys_added = 0;
        global_metrics_array[i].phys_freed = 0;
        global_metrics_array[i].logical_inserts = 0;
    }
    registered_threads_count = 0;
    
    for(int i = NUM_STRIPES - 1; i >= 0; i--) fast_unlock(&stripe_locks[i].flag);
    for(int i = NUM_ROOTS - 1; i >= 0; i--) {
        global_roots[i] = NULL;
        fast_unlock(&root_locks[i].flag);
    }
    Py_END_ALLOW_THREADS 
    Py_RETURN_NONE;
}

static PyMethodDef LQFTMethods[] = {
    {"insert", (PyCFunction)method_insert, METH_FASTCALL, "Fast-path insert single key"},
    {"search", (PyCFunction)method_search, METH_FASTCALL, "Fast-path search single key"},
    {"internal_stress_test", (PyCFunction)method_internal_stress_test, METH_FASTCALL, "Run native C stress test"},
    {"insert_batch_raw", method_insert_batch_raw, METH_VARARGS, "Bulk insert (bytes)"},
    {"search_batch", method_search_batch, METH_VARARGS, "Bulk search (list)"},
    {"get_metrics", method_get_metrics, METH_VARARGS, "Get stats"},
    {"free_all", method_free_all, METH_VARARGS, "Wipe memory"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef lqftmodule = { PyModuleDef_HEAD_INIT, "lqft_c_engine", NULL, -1, LQFTMethods };

PyMODINIT_FUNC PyInit_lqft_c_engine(void) { 
    for(int i = 0; i < NUM_ROOTS; i++) {
        global_roots[i] = NULL;
        root_locks[i].flag = 0;
    }
    
    registry = (LQFTNode**)calloc(NUM_STRIPES * STRIPE_SIZE, sizeof(LQFTNode*));
    for(int i = 0; i < NUM_STRIPES; i++) stripe_locks[i].flag = 0;
    
    // V1.0.7 FIX: Ignite the Daemon Allocator Thread
    bg_alloc_running = 1;
#ifdef _MSC_VER
    CreateThread(NULL, 0, background_alloc_thread, NULL, 0, NULL);
#else
    pthread_t bg_tid;
    pthread_create(&bg_tid, NULL, background_alloc_thread, NULL);
    pthread_detach(bg_tid); // Allow silent shutdown alongside Python interpreter
#endif

    return PyModule_Create(&lqftmodule); 
}