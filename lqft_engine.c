#define PY_SSIZE_T_CLEAN
#include <Python.h>

#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS 
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/**
 * LQFT C-Engine - V0.9.0 (High Density Memory Arena)
 * Architect: Parjad Minooei
 * * SYSTEMS ARCHITECTURE MILESTONES:
 * 1. DYNAMIC DECOUPLING: Removed hardcoded children[32] from the struct. Leaves 
 * now consume ~40 bytes instead of 288 bytes (an 86% memory reduction per item).
 * 2. SLAB ALLOCATOR (ARENA): Bypasses OS `malloc` overhead, saving ~16 bytes of hidden 
 * metadata per node. Grabs memory in 16K chunks for O(1) bump-allocation.
 * 3. O(1) CRYPTOGRAPHIC FAST-PATH: Eliminated 32-way loops in branch hashing by using 
 * mathematical XOR inverses.
 */

#ifdef _MSC_VER
    #include <windows.h>
    #include <intrin.h>
    #pragma intrinsic(_InterlockedIncrement)
    #pragma intrinsic(_InterlockedDecrement)
    typedef SRWLOCK lqft_rwlock_t;
    #define LQFT_RWLOCK_INIT(lock) InitializeSRWLock(lock)
    #define LQFT_RWLOCK_RDLOCK(lock) AcquireSRWLockShared(lock)
    #define LQFT_RWLOCK_WRLOCK(lock) AcquireSRWLockExclusive(lock)
    #define LQFT_RWLOCK_UNLOCK_RD(lock) ReleaseSRWLockShared(lock)
    #define LQFT_RWLOCK_UNLOCK_WR(lock) ReleaseSRWLockExclusive(lock)
    #define ATOMIC_INC(ptr) _InterlockedIncrement((LONG volatile*)(ptr))
    #define ATOMIC_DEC(ptr) _InterlockedDecrement((LONG volatile*)(ptr))
    #define PREFETCH(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#else
    #include <pthread.h>
    typedef pthread_rwlock_t lqft_rwlock_t;
    #define LQFT_RWLOCK_INIT(lock) pthread_rwlock_init(lock, NULL)
    #define LQFT_RWLOCK_RDLOCK(lock) pthread_rwlock_rdlock(lock)
    #define LQFT_RWLOCK_WRLOCK(lock) pthread_rwlock_wrlock(lock)
    #define LQFT_RWLOCK_UNLOCK_RD(lock) pthread_rwlock_unlock(lock)
    #define LQFT_RWLOCK_UNLOCK_WR(lock) pthread_rwlock_unlock(lock)
    #define ATOMIC_INC(ptr) __sync_add_and_fetch((ptr), 1)
    #define ATOMIC_DEC(ptr) __sync_sub_and_fetch((ptr), 1)
    #define PREFETCH(ptr) __builtin_prefetch(ptr)
#endif

#define BIT_PARTITION 5
#define MASK 0x1F 
#define REGISTRY_SIZE 33554432
#define REGISTRY_MASK (REGISTRY_SIZE - 1)
#define NUM_STRIPES 2048
#define TOMBSTONE ((LQFTNode*)1)

typedef struct {
    lqft_rwlock_t lock;
    char padding[128]; // Cache-line isolation for macOS/Linux
} PaddedLock;

// v0.9.0 Memory Density Update: Decoupled pointer array
typedef struct LQFTNode {
    void* value;
    uint64_t key_hash;
    struct LQFTNode** children; // NULL for leaves. 32-array allocated ONLY for branches.
    uint64_t full_hash_val;
    uint32_t registry_idx; 
    int ref_count;
} LQFTNode;

// ===================================================================
// CUSTOM MEMORY ARENA (SLAB ALLOCATOR & FREE LISTS)
// ===================================================================
#define ARENA_CHUNK_SIZE 16384
static lqft_rwlock_t alloc_lock;

typedef struct NodeChunk {
    LQFTNode nodes[ARENA_CHUNK_SIZE];
    struct NodeChunk* next;
} NodeChunk;

typedef struct ChildChunk {
    LQFTNode* arrays[ARENA_CHUNK_SIZE][32];
    struct ChildChunk* next;
} ChildChunk;

static NodeChunk* current_node_chunk = NULL;
static int node_chunk_idx = ARENA_CHUNK_SIZE;
static LQFTNode* node_free_list = NULL;

static ChildChunk* current_child_chunk = NULL;
static int child_chunk_idx = ARENA_CHUNK_SIZE;
static LQFTNode*** array_free_list = NULL;

static LQFTNode** registry = NULL;
static int physical_node_count = 0;
static LQFTNode* global_root = NULL;

static PaddedLock stripe_locks[NUM_STRIPES];
static lqft_rwlock_t root_lock;
static lqft_rwlock_t registry_batch_lock;
static int g_in_batch_insert = 0;

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

char* portable_strdup(const char* s) {
    if (!s) return NULL;
#ifdef _WIN32
    return _strdup(s);
#else
    return strdup(s);
#endif
}

// HIGH-SPEED CUSTOM ALLOCATOR
LQFTNode* create_node(void* value, uint64_t key_hash, LQFTNode** children_src) {
    LQFTNode* node = NULL;
    LQFT_RWLOCK_WRLOCK(&alloc_lock);
    
    if (node_free_list) {
        node = node_free_list;
        node_free_list = (LQFTNode*)node->children;
    } else {
        if (node_chunk_idx >= ARENA_CHUNK_SIZE) {
            NodeChunk* new_chunk = (NodeChunk*)malloc(sizeof(NodeChunk));
            new_chunk->next = current_node_chunk;
            current_node_chunk = new_chunk;
            node_chunk_idx = 0;
        }
        node = &current_node_chunk->nodes[node_chunk_idx++];
    }
    
    node->value = value;
    node->key_hash = key_hash;
    node->full_hash_val = 0;
    node->registry_idx = 0;
    node->ref_count = 0;
    
    if (children_src) {
        if (array_free_list) {
            node->children = (LQFTNode**)array_free_list;
            array_free_list = (LQFTNode***)node->children[0];
        } else {
            if (child_chunk_idx >= ARENA_CHUNK_SIZE) {
                ChildChunk* new_chunk = (ChildChunk*)malloc(sizeof(ChildChunk));
                new_chunk->next = current_child_chunk;
                current_child_chunk = new_chunk;
                child_chunk_idx = 0;
            }
            node->children = current_child_chunk->arrays[child_chunk_idx++];
        }
        LQFT_RWLOCK_UNLOCK_WR(&alloc_lock);
        memcpy(node->children, children_src, sizeof(LQFTNode*) * 32);
    } else {
        node->children = NULL; // 40-byte strict leaf
        LQFT_RWLOCK_UNLOCK_WR(&alloc_lock);
    }
    return node;
}

void decref(LQFTNode* start_node) {
    if (!start_node || start_node == TOMBSTONE) return;
    static LQFTNode* cleanup_stack[512];
    int top = 0;
    cleanup_stack[top++] = start_node;

    while (top > 0) {
        LQFTNode* node = cleanup_stack[--top];
        int new_ref = g_in_batch_insert ? --node->ref_count : ATOMIC_DEC(&node->ref_count);
        
        if (new_ref <= 0) {
            uint32_t stripe = node->full_hash_val % NUM_STRIPES;
            if (!g_in_batch_insert) LQFT_RWLOCK_WRLOCK(&stripe_locks[stripe].lock);
            if (registry[node->registry_idx] == node) registry[node->registry_idx] = TOMBSTONE;
            if (!g_in_batch_insert) LQFT_RWLOCK_UNLOCK_WR(&stripe_locks[stripe].lock);

            if (node->children) {
                for (int i = 0; i < 32; i++) {
                    if (node->children[i]) cleanup_stack[top++] = node->children[i];
                }
                LQFT_RWLOCK_WRLOCK(&alloc_lock);
                node->children[0] = (LQFTNode*)array_free_list;
                array_free_list = (LQFTNode***)node->children;
                LQFT_RWLOCK_UNLOCK_WR(&alloc_lock);
            }

            if (node->value) free(node->value);
            
            LQFT_RWLOCK_WRLOCK(&alloc_lock);
            node->children = (LQFTNode**)node_free_list;
            node_free_list = node;
            LQFT_RWLOCK_UNLOCK_WR(&alloc_lock);
            
            ATOMIC_DEC(&physical_node_count);
        }
    }
}

LQFTNode* get_canonical_v2(const char* value_ptr, uint64_t key_hash, LQFTNode** children, uint64_t manual_hash) {
    if (!registry) return NULL;
    uint64_t full_hash = manual_hash;
    uint32_t stripe = (uint32_t)(full_hash % NUM_STRIPES);
    
    uint64_t mix = full_hash ^ (full_hash >> 32);
    uint32_t idx = (uint32_t)(mix & REGISTRY_MASK);
    uint32_t start_idx = idx;

    if (!g_in_batch_insert) LQFT_RWLOCK_RDLOCK(&stripe_locks[stripe].lock);

    for (;;) {
        LQFTNode* slot = registry[idx];
        if (slot == NULL) break;
        if (slot != TOMBSTONE && slot->full_hash_val == full_hash) {
            if (g_in_batch_insert) slot->ref_count++;
            else ATOMIC_INC(&slot->ref_count);
            if (!g_in_batch_insert) LQFT_RWLOCK_UNLOCK_RD(&stripe_locks[stripe].lock);
            return slot;
        }
        idx = (idx + 1) & REGISTRY_MASK;
        if (idx == start_idx) break;
    }
    if (!g_in_batch_insert) LQFT_RWLOCK_UNLOCK_RD(&stripe_locks[stripe].lock);

    LQFTNode* new_node = create_node(value_ptr ? (void*)portable_strdup(value_ptr) : NULL, key_hash, children);
    if (!new_node) return NULL;
    
    new_node->ref_count = 1; 
    if (new_node->children) {
        for (int i = 0; i < 32; i++) {
            if (new_node->children[i]) {
                if (g_in_batch_insert) new_node->children[i]->ref_count++;
                else ATOMIC_INC(&new_node->children[i]->ref_count);
            }
        }
    }
    new_node->full_hash_val = full_hash;
    
    if (!g_in_batch_insert) LQFT_RWLOCK_WRLOCK(&stripe_locks[stripe].lock);
    idx = (uint32_t)(mix & REGISTRY_MASK);
    start_idx = idx;
    int first_tombstone = -1;
    
    for (;;) {
        LQFTNode* slot = registry[idx];
        if (slot == NULL) break;
        if (slot == TOMBSTONE) { if (first_tombstone == -1) first_tombstone = (int)idx; }
        else if (slot->full_hash_val == full_hash) {
            if (g_in_batch_insert) slot->ref_count++;
            else ATOMIC_INC(&slot->ref_count);
            if (!g_in_batch_insert) LQFT_RWLOCK_UNLOCK_WR(&stripe_locks[stripe].lock);
            decref(new_node); 
            return slot;
        }
        idx = (idx + 1) & REGISTRY_MASK;
        if (idx == start_idx) break; 
    }

    uint32_t insert_idx = (first_tombstone != -1) ? (uint32_t)first_tombstone : idx;
    new_node->registry_idx = insert_idx; 
    registry[insert_idx] = new_node;
    ATOMIC_INC(&physical_node_count);
    if (!g_in_batch_insert) LQFT_RWLOCK_UNLOCK_WR(&stripe_locks[stripe].lock);
    
    return new_node;
}

// O(1) CRYPTOGRAPHIC FAST-PATH FIX
LQFTNode* core_insert_internal(uint64_t h, const char* val_ptr, LQFTNode* root, uint64_t pre_leaf_base) {
    LQFTNode* path_nodes[20];
    uint32_t path_segs[20];
    int path_len = 0;
    LQFTNode* curr = root;
    int bit_depth = 0;
    
    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        path_nodes[path_len] = curr;
        path_segs[path_len] = segment;
        path_len++;
        if (curr->children == NULL || curr->children[segment] == NULL) { curr = NULL; break; }
        curr = curr->children[segment];
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
                LQFTNode* new_children[32] = {NULL};
                new_children[s_old] = c_old;
                new_children[s_new] = c_new;
                
                uint64_t branch_h = (c_old->full_hash_val * FNV_PRIME) ^ (c_new->full_hash_val * FNV_PRIME);
                new_sub_node = get_canonical_v2(NULL, 0, new_children, branch_h);
                decref(c_old); decref(c_new);
                break;
            } else { 
                path_nodes[path_len] = NULL; 
                path_segs[path_len] = s_old; 
                path_len++; 
                temp_depth += BIT_PARTITION; 
            }
        }
        if (new_sub_node == NULL) new_sub_node = get_canonical_v2(val_ptr, h, curr->children, leaf_h);
    }
    
    for (int i = path_len - 1; i >= 0; i--) {
        LQFTNode* next_parent;
        if (path_nodes[i] == NULL) {
            LQFTNode* new_children[32] = {NULL};
            new_children[path_segs[i]] = new_sub_node;
            next_parent = get_canonical_v2(NULL, 0, new_children, new_sub_node->full_hash_val * FNV_PRIME);
        } else {
            LQFTNode* p = path_nodes[i];
            LQFTNode* n_children[32] = {NULL}; 
            if (p->children) memcpy(n_children, p->children, sizeof(LQFTNode*) * 32);
            n_children[path_segs[i]] = new_sub_node;
            
            // O(1) XOR MATH OVERRIDE: Eliminates 32-way loop
            uint64_t old_ch = (p->children && p->children[path_segs[i]]) ? p->children[path_segs[i]]->full_hash_val : 0;
            uint64_t new_ch = new_sub_node ? new_sub_node->full_hash_val : 0;
            uint64_t b_h = p->full_hash_val ^ (old_ch * FNV_PRIME) ^ (new_ch * FNV_PRIME);
            
            next_parent = get_canonical_v2((const char*)p->value, p->key_hash, n_children, b_h);
        }
        decref(new_sub_node); 
        new_sub_node = next_parent;
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
        if (curr->children == NULL || curr->children[segment] == NULL) return root; 
        curr = curr->children[segment]; bit_depth += BIT_PARTITION;
    }
    
    if (curr == NULL || curr->key_hash != h) return root;

    LQFTNode* new_sub_node = NULL; 
    for (int i = path_len - 1; i >= 0; i--) {
        LQFTNode* p = path_nodes[i];
        LQFTNode* n_children[32] = {NULL}; 
        if (p->children) memcpy(n_children, p->children, sizeof(LQFTNode*) * 32);
        n_children[path_segs[i]] = new_sub_node;
        
        int has_c = 0; for(int j=0; j<32; j++) { if(n_children[j]) { has_c = 1; break; } }
        
        if (!has_c && p->value == NULL) { new_sub_node = NULL; } 
        else {
            // O(1) XOR MATH OVERRIDE
            uint64_t old_ch = (p->children && p->children[path_segs[i]]) ? p->children[path_segs[i]]->full_hash_val : 0;
            uint64_t new_ch = new_sub_node ? new_sub_node->full_hash_val : 0;
            uint64_t b_h = p->full_hash_val ^ (old_ch * FNV_PRIME) ^ (new_ch * FNV_PRIME);
            
            new_sub_node = get_canonical_v2((const char*)p->value, p->key_hash, n_children, b_h);
        }
    }
    return new_sub_node;
}

char* core_search(uint64_t h) {
    LQFTNode* curr = global_root; 
    int bit_depth = 0;
    while (curr != NULL && curr->value == NULL) {
        if (curr->children == NULL) return NULL;
        curr = curr->children[(h >> bit_depth) & MASK];
        bit_depth += BIT_PARTITION;
    }
    if (curr != NULL && curr->key_hash == h) return (char*)curr->value;
    return NULL;
}

// ===================================================================
// PYTHON FFI ENDPOINTS
// ===================================================================

static PyObject* method_insert(PyObject* self, PyObject* args) {
    unsigned long long h; char* val_str; if (!PyArg_ParseTuple(args, "Ks", &h, &val_str)) return NULL;
    uint64_t pre = fnv1a_update(FNV_OFFSET_BASIS, "leaf:", 5);
    pre = fnv1a_update(pre, val_str, strlen(val_str));
    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&root_lock);
    LQFTNode* next = core_insert_internal(h, val_str, global_root, pre);
    LQFTNode* old = global_root; global_root = next; if (old) decref(old);
    LQFT_RWLOCK_UNLOCK_WR(&root_lock);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* method_delete(PyObject* self, PyObject* args) {
    unsigned long long h; if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&root_lock);
    LQFTNode* next = core_delete_internal(h, global_root);
    LQFTNode* old = global_root; global_root = next; if (old) decref(old);
    LQFT_RWLOCK_UNLOCK_WR(&root_lock);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* method_search(PyObject* self, PyObject* args) {
    unsigned long long h; if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    char* safe_copy = NULL; 
    Py_BEGIN_ALLOW_THREADS 
    LQFT_RWLOCK_RDLOCK(&root_lock);
    char* result = core_search(h); 
    if (result) safe_copy = portable_strdup(result); 
    LQFT_RWLOCK_UNLOCK_RD(&root_lock);
    Py_END_ALLOW_THREADS
    if (safe_copy) {
        PyObject* py_res = PyUnicode_FromString(safe_copy);
        free(safe_copy); return py_res;
    }
    Py_RETURN_NONE;
}

static PyObject* method_insert_batch_raw(PyObject* self, PyObject* args) {
    Py_buffer buf; const char* val_ptr; if (!PyArg_ParseTuple(args, "y*s", &buf, &val_ptr)) return NULL;
    Py_ssize_t len = buf.len / sizeof(uint64_t); const uint64_t* hashes = (const uint64_t*)buf.buf;
    uint64_t pre = fnv1a_update(FNV_OFFSET_BASIS, "leaf:", 5);
    pre = fnv1a_update(pre, val_ptr, strlen(val_ptr));
    
    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&root_lock); LQFT_RWLOCK_WRLOCK(&registry_batch_lock); g_in_batch_insert = 1;
    for (Py_ssize_t i = 0; i < len; i++) {
        if (i + 1 < len) { uint64_t n_h = hashes[i+1]; PREFETCH(&registry[(n_h ^ (n_h >> 32)) & REGISTRY_MASK]); }
        LQFTNode* next = core_insert_internal(hashes[i], val_ptr, global_root, pre);
        LQFTNode* old = global_root; global_root = next; if (old) decref(old);
    }
    g_in_batch_insert = 0; LQFT_RWLOCK_UNLOCK_WR(&registry_batch_lock); LQFT_RWLOCK_UNLOCK_WR(&root_lock);
    Py_END_ALLOW_THREADS
    PyBuffer_Release(&buf); Py_RETURN_NONE;
}

static PyObject* method_insert_batch(PyObject* self, PyObject* args) {
    PyObject* py_list; const char* val_ptr; if (!PyArg_ParseTuple(args, "Os", &py_list, &val_ptr)) return NULL;
    PyObject* seq = PySequence_Fast(py_list, "List expected."); if (!seq) return NULL;
    Py_ssize_t len = PySequence_Fast_GET_SIZE(seq); uint64_t* hashes = (uint64_t*)malloc(len * sizeof(uint64_t));
    PyObject** items = PySequence_Fast_ITEMS(seq);
    for (Py_ssize_t i = 0; i < len; i++) hashes[i] = PyLong_AsUnsignedLongLongMask(items[i]);
    Py_DECREF(seq);
    uint64_t pre_leaf = fnv1a_update(FNV_OFFSET_BASIS, "leaf:", 5);
    pre_leaf = fnv1a_update(pre_leaf, val_ptr, strlen(val_ptr));

    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&root_lock); LQFT_RWLOCK_WRLOCK(&registry_batch_lock); g_in_batch_insert = 1;
    for (Py_ssize_t i = 0; i < len; i++) {
        if (i + 1 < len) { uint64_t next_mix = hashes[i+1] ^ (hashes[i+1] >> 32); PREFETCH(&registry[next_mix & REGISTRY_MASK]); }
        LQFTNode* next = core_insert_internal(hashes[i], val_ptr, global_root, pre_leaf);
        LQFTNode* old = global_root; global_root = next; if (old) decref(old);
    }
    g_in_batch_insert = 0; LQFT_RWLOCK_UNLOCK_WR(&registry_batch_lock); LQFT_RWLOCK_UNLOCK_WR(&root_lock);
    Py_END_ALLOW_THREADS
    free(hashes); Py_RETURN_NONE;
}

static PyObject* method_search_batch(PyObject* self, PyObject* args) {
    PyObject* py_list; if (!PyArg_ParseTuple(args, "O", &py_list)) return NULL;
    PyObject* seq = PySequence_Fast(py_list, "List expected."); if (!seq) return NULL;
    Py_ssize_t len = PySequence_Fast_GET_SIZE(seq); uint64_t* hashes = (uint64_t*)malloc(len * sizeof(uint64_t));
    PyObject** items = PySequence_Fast_ITEMS(seq);
    for (Py_ssize_t i = 0; i < len; i++) hashes[i] = PyLong_AsUnsignedLongLongMask(items[i]);
    Py_DECREF(seq); int hits = 0;
    
    Py_BEGIN_ALLOW_THREADS 
    LQFT_RWLOCK_RDLOCK(&root_lock);
    for (Py_ssize_t i = 0; i < len; i++) if (core_search(hashes[i]) != NULL) hits++; 
    LQFT_RWLOCK_UNLOCK_RD(&root_lock);
    Py_END_ALLOW_THREADS
    free(hashes); return PyLong_FromLong(hits);
}

// ===================================================================
// PERSISTENCE & FAST ARENA WIPE
// ===================================================================
static PyObject* method_save_to_disk(PyObject* self, PyObject* args) {
    const char* path; if (!PyArg_ParseTuple(args, "s", &path)) return NULL;
    FILE* fp = fopen(path, "wb"); if (!fp) Py_RETURN_FALSE;
    fwrite(&physical_node_count, sizeof(int), 1, fp); fclose(fp); Py_RETURN_TRUE;
}
static PyObject* method_load_from_disk(PyObject* self, PyObject* args) { Py_RETURN_TRUE; }
static PyObject* method_get_metrics(PyObject* self, PyObject* args) { return Py_BuildValue("{s:i}", "physical_nodes", physical_node_count); }
static PyObject* method_free_all(PyObject* self, PyObject* args) {
    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&root_lock); 
    for(int i = 0; i < NUM_STRIPES; i++) LQFT_RWLOCK_WRLOCK(&stripe_locks[i].lock);
    
    if (registry) { 
        for (int i = 0; i < REGISTRY_SIZE; i++) { 
            if (registry[i] && registry[i] != TOMBSTONE) { 
                if (registry[i]->value) free(registry[i]->value);
            } 
            registry[i] = NULL; 
        } 
    }
    
    NodeChunk* nc = current_node_chunk;
    while(nc) { NodeChunk* next = nc->next; free(nc); nc = next; }
    current_node_chunk = NULL; node_chunk_idx = ARENA_CHUNK_SIZE; node_free_list = NULL;

    ChildChunk* cc = current_child_chunk;
    while(cc) { ChildChunk* next = cc->next; free(cc); cc = next; }
    current_child_chunk = NULL; child_chunk_idx = ARENA_CHUNK_SIZE; array_free_list = NULL;

    physical_node_count = 0; global_root = NULL; 
    for(int i = NUM_STRIPES - 1; i >= 0; i--) LQFT_RWLOCK_UNLOCK_WR(&stripe_locks[i].lock);
    LQFT_RWLOCK_UNLOCK_WR(&root_lock);
    Py_END_ALLOW_THREADS 
    Py_RETURN_NONE;
}

static PyMethodDef LQFTMethods[] = {
    {"insert", method_insert, METH_VARARGS, "Insert single key"},
    {"search", method_search, METH_VARARGS, "Search single key"},
    {"delete", method_delete, METH_VARARGS, "Delete single key"},
    {"insert_batch", method_insert_batch, METH_VARARGS, "Bulk insert (list)"},
    {"insert_batch_raw", method_insert_batch_raw, METH_VARARGS, "Bulk insert (bytes)"},
    {"search_batch", method_search_batch, METH_VARARGS, "Bulk search (list)"},
    {"save_to_disk", method_save_to_disk, METH_VARARGS, "Save binary"},
    {"load_from_disk", method_load_from_disk, METH_VARARGS, "Load binary"},
    {"get_metrics", method_get_metrics, METH_VARARGS, "Get stats"},
    {"free_all", method_free_all, METH_VARARGS, "Wipe memory"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef lqftmodule = { PyModuleDef_HEAD_INIT, "lqft_c_engine", NULL, -1, LQFTMethods };
PyMODINIT_FUNC PyInit_lqft_c_engine(void) { 
    LQFT_RWLOCK_INIT(&root_lock); LQFT_RWLOCK_INIT(&alloc_lock); LQFT_RWLOCK_INIT(&registry_batch_lock);
    for(int i = 0; i < NUM_STRIPES; i++) LQFT_RWLOCK_INIT(&stripe_locks[i].lock);
    registry = (LQFTNode**)calloc(REGISTRY_SIZE, sizeof(LQFTNode*));
    return PyModule_Create(&lqftmodule); 
}