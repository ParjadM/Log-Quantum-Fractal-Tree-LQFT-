#define PY_SSIZE_T_CLEAN
#include <Python.h>

#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS 
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

// --- PHASE 2: CROSS-PLATFORM HARDWARE LOCKS ---
#ifdef _WIN32
    #include <windows.h>
    typedef SRWLOCK lqft_rwlock_t;
    #define LQFT_RWLOCK_INIT(lock) InitializeSRWLock(lock)
    #define LQFT_RWLOCK_RDLOCK(lock) AcquireSRWLockShared(lock)
    #define LQFT_RWLOCK_WRLOCK(lock) AcquireSRWLockExclusive(lock)
    #define LQFT_RWLOCK_UNLOCK_RD(lock) ReleaseSRWLockShared(lock)
    #define LQFT_RWLOCK_UNLOCK_WR(lock) ReleaseSRWLockExclusive(lock)
#else
    #include <pthread.h>
    typedef pthread_rwlock_t lqft_rwlock_t;
    #define LQFT_RWLOCK_INIT(lock) pthread_rwlock_init(lock, NULL)
    #define LQFT_RWLOCK_RDLOCK(lock) pthread_rwlock_rdlock(lock)
    #define LQFT_RWLOCK_WRLOCK(lock) pthread_rwlock_wrlock(lock)
    #define LQFT_RWLOCK_UNLOCK_RD(lock) pthread_rwlock_unlock(lock)
    #define LQFT_RWLOCK_UNLOCK_WR(lock) pthread_rwlock_unlock(lock)
#endif

/**
 * LQFT C-Engine - V0.6.0 (Hardware Concurrency)
 * Architect: Parjad Minooei
 * * CHANGE LOG:
 * - Implemented SRWLOCK / pthread_rwlock for true multi-core utilization.
 * - Bypassed Python GIL using Py_BEGIN_ALLOW_THREADS.
 * - Fixed Macro brace expansions for thread safe early returns.
 */

#define BIT_PARTITION 5
#define MAX_BITS 64 
#define MASK 0x1F 
#define REGISTRY_SIZE 8000009 
#define TOMBSTONE ((LQFTNode*)1)

typedef struct LQFTNode {
    void* value;
    uint64_t key_hash;
    struct LQFTNode* children[32]; 
    char struct_hash[17]; 
    uint64_t full_hash_val;
    int ref_count;
} LQFTNode;

static LQFTNode** registry = NULL;
static int physical_node_count = 0;
static LQFTNode* global_root = NULL;
static lqft_rwlock_t engine_lock; // The Master Hardware Lock
static int lock_initialized = 0;

const uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
const uint64_t FNV_PRIME = 1099511628211ULL;

// -------------------------------------------------------------------
// Utilities
// -------------------------------------------------------------------
uint64_t fnv1a_update(uint64_t hash, const void* data, size_t len) {
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

static int init_registry() {
    if (!lock_initialized) {
        LQFT_RWLOCK_INIT(&engine_lock);
        lock_initialized = 1;
    }
    if (registry == NULL) {
        registry = (LQFTNode**)calloc(REGISTRY_SIZE, sizeof(LQFTNode*));
        if (registry == NULL) return 0;
    }
    return 1;
}

LQFTNode* create_node(void* value, uint64_t key_hash) {
    LQFTNode* node = (LQFTNode*)malloc(sizeof(LQFTNode));
    if (!node) return NULL;
    node->value = value;
    node->key_hash = key_hash;
    node->full_hash_val = 0;
    node->ref_count = 0;
    for (int i = 0; i < 32; i++) node->children[i] = NULL;
    return node;
}

// -------------------------------------------------------------------
// Memory Management (ARC)
// -------------------------------------------------------------------
void decref(LQFTNode* node) {
    if (!node) return;
    node->ref_count--;
    if (node->ref_count <= 0) {
        for (int i = 0; i < 32; i++) {
            if (node->children[i]) decref(node->children[i]);
        }
        uint32_t idx = node->full_hash_val % REGISTRY_SIZE;
        uint32_t start_idx = idx;
        while (registry[idx] != NULL) {
            if (registry[idx] == node) {
                registry[idx] = TOMBSTONE;
                break;
            }
            idx = (idx + 1) % REGISTRY_SIZE;
            if (idx == start_idx) break;
        }
        if (node->value) free(node->value);
        free(node);
        physical_node_count--;
    }
}

static PyObject* method_free_all(PyObject* self, PyObject* args) {
    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&engine_lock);
    if (registry != NULL) {
        for (int i = 0; i < REGISTRY_SIZE; i++) {
            if (registry[i] != NULL && registry[i] != TOMBSTONE) {
                if (registry[i]->value) free(registry[i]->value);
                free(registry[i]);
            }
            registry[i] = NULL;
        }
        free(registry);
        registry = NULL;
    }
    physical_node_count = 0;
    global_root = NULL;
    LQFT_RWLOCK_UNLOCK_WR(&engine_lock);
    Py_END_ALLOW_THREADS

    Py_RETURN_NONE;
}

// -------------------------------------------------------------------
// Disk Persistence (Binary Serialization)
// -------------------------------------------------------------------
static PyObject* method_save_to_disk(PyObject* self, PyObject* args) {
    const char* filepath;
    if (!PyArg_ParseTuple(args, "s", &filepath)) return NULL;
    
    int success = 1;
    
    // Release GIL, Lock Engine for Writing
    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&engine_lock);
    
    if (!registry) {
        success = 0;
    } else {
        FILE* fp = fopen(filepath, "wb");
        if (!fp) {
            success = 0;
        } else {
            char magic[4] = "LQFT";
            fwrite(magic, 1, 4, fp);
            fwrite(&physical_node_count, sizeof(int), 1, fp);
            uint64_t root_hash = global_root ? global_root->full_hash_val : 0;
            fwrite(&root_hash, sizeof(uint64_t), 1, fp);

            for (int i = 0; i < REGISTRY_SIZE; i++) {
                LQFTNode* node = registry[i];
                if (node != NULL && node != TOMBSTONE) {
                    fwrite(&node->full_hash_val, sizeof(uint64_t), 1, fp);
                    fwrite(&node->key_hash, sizeof(uint64_t), 1, fp);
                    fwrite(node->struct_hash, 1, 17, fp);
                    fwrite(&node->ref_count, sizeof(int), 1, fp);

                    int has_val = (node->value != NULL) ? 1 : 0;
                    fwrite(&has_val, sizeof(int), 1, fp);
                    if (has_val) {
                        int v_len = (int)strlen((char*)node->value);
                        fwrite(&v_len, sizeof(int), 1, fp);
                        fwrite(node->value, 1, v_len, fp);
                    }

                    uint64_t child_refs[32] = {0};
                    for (int c = 0; c < 32; c++) {
                        if (node->children[c]) child_refs[c] = node->children[c]->full_hash_val;
                    }
                    fwrite(child_refs, sizeof(uint64_t), 32, fp);
                }
            }
            fclose(fp);
        }
    }
    LQFT_RWLOCK_UNLOCK_WR(&engine_lock);
    Py_END_ALLOW_THREADS

    if (!success) return PyErr_SetFromErrno(PyExc_IOError);
    Py_RETURN_TRUE;
}

LQFTNode* find_in_registry(uint64_t full_hash) {
    if (full_hash == 0) return NULL;
    uint32_t idx = full_hash % REGISTRY_SIZE;
    uint32_t start_idx = idx;
    while (registry[idx] != NULL) {
        if (registry[idx] != TOMBSTONE && registry[idx]->full_hash_val == full_hash) {
            return registry[idx];
        }
        idx = (idx + 1) % REGISTRY_SIZE;
        if (idx == start_idx) break;
    }
    return NULL;
}

static PyObject* method_load_from_disk(PyObject* self, PyObject* args) {
    const char* filepath;
    if (!PyArg_ParseTuple(args, "s", &filepath)) return NULL;

    int success = 1;
    int format_error = 0;

    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&engine_lock);
    
    FILE* fp = fopen(filepath, "rb");
    if (!fp) {
        success = 0;
    } else {
        char magic[5] = {0};
        fread(magic, 1, 4, fp);
        if (strcmp(magic, "LQFT") != 0) {
            fclose(fp);
            format_error = 1;
            success = 0;
        } else {
            // Internal clear
            if (registry != NULL) {
                for (int i = 0; i < REGISTRY_SIZE; i++) {
                    if (registry[i] != NULL && registry[i] != TOMBSTONE) {
                        if (registry[i]->value) free(registry[i]->value);
                        free(registry[i]);
                    }
                    registry[i] = NULL;
                }
            }
            physical_node_count = 0;
            global_root = NULL;
            init_registry();

            int total_nodes;
            uint64_t root_hash;
            fread(&total_nodes, sizeof(int), 1, fp);
            fread(&root_hash, sizeof(uint64_t), 1, fp);

            uint64_t* all_child_refs = (uint64_t*)malloc(total_nodes * 32 * sizeof(uint64_t));
            LQFTNode** loaded_nodes = (LQFTNode**)malloc(total_nodes * sizeof(LQFTNode*));

            for (int i = 0; i < total_nodes; i++) {
                LQFTNode* node = create_node(NULL, 0);
                fread(&node->full_hash_val, sizeof(uint64_t), 1, fp);
                fread(&node->key_hash, sizeof(uint64_t), 1, fp);
                fread(node->struct_hash, 1, 17, fp);
                fread(&node->ref_count, sizeof(int), 1, fp);

                int has_val;
                fread(&has_val, sizeof(int), 1, fp);
                if (has_val) {
                    int v_len;
                    fread(&v_len, sizeof(int), 1, fp);
                    char* val_str = (char*)malloc(v_len + 1);
                    fread(val_str, 1, v_len, fp);
                    val_str[v_len] = '\0';
                    node->value = val_str;
                }

                fread(&all_child_refs[i * 32], sizeof(uint64_t), 32, fp);

                uint32_t idx = node->full_hash_val % REGISTRY_SIZE;
                while (registry[idx] != NULL) idx = (idx + 1) % REGISTRY_SIZE;
                registry[idx] = node;
                loaded_nodes[i] = node;
                physical_node_count++;
            }

            for (int i = 0; i < total_nodes; i++) {
                for (int c = 0; c < 32; c++) {
                    uint64_t target_hash = all_child_refs[i * 32 + c];
                    if (target_hash != 0) loaded_nodes[i]->children[c] = find_in_registry(target_hash);
                }
            }

            global_root = find_in_registry(root_hash);
            free(all_child_refs);
            free(loaded_nodes);
            fclose(fp);
        }
    }
    LQFT_RWLOCK_UNLOCK_WR(&engine_lock);
    Py_END_ALLOW_THREADS

    if (format_error) {
        PyErr_SetString(PyExc_ValueError, "Invalid LQFT binary file format.");
        return NULL;
    }
    if (!success) return PyErr_SetFromErrno(PyExc_IOError);

    Py_RETURN_TRUE;
}

// -------------------------------------------------------------------
// Merkle-DAG Core (Standard Operations)
// -------------------------------------------------------------------
LQFTNode* get_canonical(void* value, uint64_t key_hash, LQFTNode** children) {
    if (!init_registry()) return NULL;

    uint64_t full_hash = FNV_OFFSET_BASIS;
    if (value != NULL) {
        full_hash = fnv1a_update(full_hash, "leaf:", 5);
        full_hash = fnv1a_update(full_hash, value, strlen((char*)value));
        full_hash = fnv1a_update(full_hash, &key_hash, sizeof(uint64_t));
    } else {
        full_hash = fnv1a_update(full_hash, "branch:", 7);
        if (children) {
            for (int i = 0; i < 32; i++) {
                if (children[i]) {
                    full_hash = fnv1a_update(full_hash, &i, sizeof(int));
                    full_hash = fnv1a_update(full_hash, children[i]->struct_hash, 16);
                }
            }
        }
    }
    
    char lookup_hash[17];
    sprintf(lookup_hash, "%016llx", (unsigned long long)full_hash);
    uint32_t idx = full_hash % REGISTRY_SIZE;
    uint32_t start_idx = idx;
    int first_tombstone = -1;

    while (registry[idx] != NULL) {
        if (registry[idx] == TOMBSTONE) {
            if (first_tombstone == -1) first_tombstone = (int)idx;
        } else if (registry[idx]->full_hash_val == full_hash && strcmp(registry[idx]->struct_hash, lookup_hash) == 0) {
            if (value) free(value);
            return registry[idx];
        }
        idx = (idx + 1) % REGISTRY_SIZE;
        if (idx == start_idx) break; 
    }

    LQFTNode* new_node = create_node(value, key_hash);
    if (!new_node) return NULL;
    
    if (children) {
        for (int i = 0; i < 32; i++) {
            new_node->children[i] = children[i];
            if (children[i]) children[i]->ref_count++; 
        }
    }
    
    strcpy(new_node->struct_hash, lookup_hash);
    new_node->full_hash_val = full_hash;
    
    uint32_t insert_idx = (first_tombstone != -1) ? (uint32_t)first_tombstone : idx;
    registry[insert_idx] = new_node;
    physical_node_count++;
    
    return new_node;
}

static PyObject* method_insert(PyObject* self, PyObject* args) {
    unsigned long long h;
    char* val_str;
    if (!PyArg_ParseTuple(args, "Ks", &h, &val_str)) return NULL;
    
    // Copy the string before dropping the GIL to prevent memory corruption
    char* val_copy = portable_strdup(val_str);

    // Bypass GIL & Lock Engine (Exclusive Write Lock)
    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&engine_lock);
    
    if (!global_root) {
        init_registry();
        global_root = get_canonical(NULL, 0, NULL);
        global_root->ref_count++;
    }
    
    LQFTNode* old_root = global_root;
    LQFTNode* path_nodes[20];
    uint32_t path_segs[20];
    int path_len = 0;
    LQFTNode* curr = global_root;
    int bit_depth = 0;
    
    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        path_nodes[path_len] = curr;
        path_segs[path_len] = segment;
        path_len++;
        if (curr->children[segment] == NULL) { curr = NULL; break; }
        curr = curr->children[segment];
        bit_depth += BIT_PARTITION;
    }
    
    LQFTNode* new_sub_node = NULL;
    if (curr == NULL) { 
        new_sub_node = get_canonical(portable_strdup(val_copy), h, NULL); 
    } else if (curr->key_hash == h) { 
        new_sub_node = get_canonical(portable_strdup(val_copy), h, curr->children); 
    } else {
        unsigned long long old_h = curr->key_hash;
        char* old_val = portable_strdup((char*)curr->value);
        int temp_depth = bit_depth;
        while (temp_depth < 64) {
            uint32_t s_old = (old_h >> temp_depth) & MASK;
            uint32_t s_new = (h >> temp_depth) & MASK;
            if (s_old != s_new) {
                LQFTNode* c_old = get_canonical(old_val, old_h, curr->children);
                LQFTNode* c_new = get_canonical(portable_strdup(val_copy), h, NULL);
                LQFTNode* new_children[32] = {NULL};
                new_children[s_old] = c_old;
                new_children[s_new] = c_new;
                new_sub_node = get_canonical(NULL, 0, new_children);
                break;
            } else { 
                path_nodes[path_len] = NULL; 
                path_segs[path_len] = s_old; 
                path_len++; 
                temp_depth += BIT_PARTITION; 
            }
        }
        if (new_sub_node == NULL) new_sub_node = get_canonical(portable_strdup(val_copy), h, curr->children);
    }
    
    for (int i = path_len - 1; i >= 0; i--) {
        if (path_nodes[i] == NULL) {
            LQFTNode* new_children[32] = {NULL};
            new_children[path_segs[i]] = new_sub_node;
            new_sub_node = get_canonical(NULL, 0, new_children);
        } else {
            LQFTNode* p_node = path_nodes[i];
            uint32_t segment = path_segs[i];
            LQFTNode* new_children[32];
            for (int j = 0; j < 32; j++) new_children[j] = p_node->children[j];
            new_children[segment] = new_sub_node;
            new_sub_node = get_canonical(p_node->value, p_node->key_hash, new_children);
        }
    }
    
    global_root = new_sub_node;
    global_root->ref_count++;
    if (old_root) decref(old_root); 
    
    free(val_copy);
    LQFT_RWLOCK_UNLOCK_WR(&engine_lock);
    Py_END_ALLOW_THREADS

    Py_RETURN_NONE;
}

static PyObject* method_search(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    
    char* result_str = NULL;

    // Bypass GIL & Lock Engine (Shared Read Lock - Multiple threads can enter simultaneously!)
    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_RDLOCK(&engine_lock);
    
    if (global_root) {
        LQFTNode* curr = global_root;
        int bit_depth = 0;
        while (curr != NULL && curr->value == NULL) {
            uint32_t segment = (h >> bit_depth) & MASK;
            curr = curr->children[segment];
            bit_depth += BIT_PARTITION;
        }
        if (curr != NULL && curr->key_hash == h) {
            result_str = (char*)curr->value;
        }
    }
    
    LQFT_RWLOCK_UNLOCK_RD(&engine_lock);
    Py_END_ALLOW_THREADS
    
    if (result_str) return PyUnicode_FromString(result_str);
    Py_RETURN_NONE;
}

static PyObject* method_delete(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;

    Py_BEGIN_ALLOW_THREADS
    LQFT_RWLOCK_WRLOCK(&engine_lock);
    
    if (global_root) {
        LQFTNode* path_nodes[20]; 
        uint32_t path_segs[20];
        int path_len = 0;
        LQFTNode* curr = global_root;
        int bit_depth = 0;

        while (curr != NULL && curr->value == NULL) {
            uint32_t segment = (h >> bit_depth) & MASK;
            path_nodes[path_len] = curr;
            path_segs[path_len] = segment;
            path_len++;
            curr = curr->children[segment];
            bit_depth += BIT_PARTITION;
        }

        if (curr != NULL && curr->key_hash == h) {
            LQFTNode* old_root = global_root;
            LQFTNode* new_sub_node = NULL;

            for (int i = path_len - 1; i >= 0; i--) {
                LQFTNode* p_node = path_nodes[i];
                uint32_t segment = path_segs[i];
                LQFTNode* new_children[32];
                int has_other_children = 0;

                for (uint32_t j = 0; j < 32; j++) {
                    if (j == segment) new_children[j] = new_sub_node;
                    else {
                        new_children[j] = p_node->children[j];
                        if (new_children[j]) has_other_children = 1;
                    }
                }

                if (!has_other_children && i > 0) new_sub_node = NULL;
                else new_sub_node = get_canonical(NULL, 0, new_children);
            }

            global_root = (new_sub_node) ? new_sub_node : get_canonical(NULL, 0, NULL);
            if (global_root) global_root->ref_count++;
            if (old_root) decref(old_root); 
        }
    }
    
    LQFT_RWLOCK_UNLOCK_WR(&engine_lock);
    Py_END_ALLOW_THREADS

    Py_RETURN_NONE;
}

static PyObject* method_get_metrics(PyObject* self, PyObject* args) {
    return Py_BuildValue("{s:i}", "physical_nodes", physical_node_count);
}

static PyMethodDef LQFTMethods[] = {
    {"insert", method_insert, METH_VARARGS, "Insert key-value"},
    {"delete", method_delete, METH_VARARGS, "Delete key"},
    {"search", method_search, METH_VARARGS, "Search key"},
    {"save_to_disk", method_save_to_disk, METH_VARARGS, "Serialize to .bin"},
    {"load_from_disk", method_load_from_disk, METH_VARARGS, "Deserialize from .bin"},
    {"get_metrics", method_get_metrics, METH_VARARGS, "Get stats"},
    {"free_all", method_free_all, METH_VARARGS, "Total memory wipe"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef lqftmodule = { PyModuleDef_HEAD_INIT, "lqft_c_engine", NULL, -1, LQFTMethods };
PyMODINIT_FUNC PyInit_lqft_c_engine(void) { 
    LQFT_RWLOCK_INIT(&engine_lock);
    lock_initialized = 1;
    return PyModule_Create(&lqftmodule); 
}