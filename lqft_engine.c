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
 * LQFT C-Engine (Log-Quantum Fractal Tree) - V4.2 (Memory Safe Master Build)
 * Architect: Parjad Minooei
 * * INTEGRATED FIXES:
 * - 64-bit FNV-1a structural hashing (solves 32-bit collisions).
 * - Segment Indexing [i] to branch hashing (solves Ghost Sibling merges).
 * - Capped MAX_BITS to 64 to prevent Undefined Behavior.
 * - NEW: Global Registry Purge (free_all) for memory reclamation.
 */

#define BIT_PARTITION 5
#define MAX_BITS 64 
#define MASK 0x1F 
#define REGISTRY_SIZE 8000009 // 8 Million prime slots

typedef struct LQFTNode {
    void* value;
    uint64_t key_hash;
    struct LQFTNode* children[32]; 
    char struct_hash[17]; // 16 Hex chars + 1 Null terminator
} LQFTNode;

// Global Registry for Interning and Memory Management
static LQFTNode* registry[REGISTRY_SIZE];
static int physical_node_count = 0;
static LQFTNode* global_root = NULL;

// Upgraded to 64-bit FNV-1a Hash
uint64_t fnv1a_64(const char* str) {
    uint64_t hash = 14695981039346656037ULL;
    while (*str) {
        hash ^= (uint8_t)(*str++);
        hash *= 1099511628211ULL;
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

LQFTNode* create_node(void* value, uint64_t key_hash) {
    LQFTNode* node = (LQFTNode*)malloc(sizeof(LQFTNode));
    node->value = value;
    node->key_hash = key_hash;
    for (int i = 0; i < 32; i++) node->children[i] = NULL;
    return node;
}

LQFTNode* get_canonical(void* value, uint64_t key_hash, LQFTNode** children) {
    char buffer[8192] = { 0 };
    if (value != NULL) {
        sprintf(buffer, "leaf:%s:%llu", (char*)value, (unsigned long long)key_hash);
    } else {
        sprintf(buffer, "branch:");
        for (int i = 0; i < 32; i++) {
            if (children && children[i]) {
                char seg_buf[32];
                // FIX: Adding [%d] prevents identically hashed children in different slots from merging
                sprintf(seg_buf, "[%d]%s", i, children[i]->struct_hash);
                strcat(buffer, seg_buf);
            }
        }
    }
    
    uint64_t full_hash = fnv1a_64(buffer);
    char lookup_hash[17];
    sprintf(lookup_hash, "%016llx", (unsigned long long)full_hash);
    uint32_t idx = full_hash % REGISTRY_SIZE;

    uint32_t start_idx = idx;
    while (registry[idx] != NULL) {
        if (strcmp(registry[idx]->struct_hash, lookup_hash) == 0) {
            // Found existing node; free the duplicated value string if provided
            if (value) free(value); 
            return registry[idx];
        }
        idx = (idx + 1) % REGISTRY_SIZE;
        if (idx == start_idx) break; 
    }

    LQFTNode* new_node = create_node(value, key_hash);
    if (children) {
        for (int i = 0; i < 32; i++) new_node->children[i] = children[i];
    }
    strcpy(new_node->struct_hash, lookup_hash);
    registry[idx] = new_node;
    physical_node_count++;
    return new_node;
}

// --- MEMORY RECLAMATION ---

static PyObject* method_free_all(PyObject* self, PyObject* args) {
    int freed_count = 0;
    for (int i = 0; i < REGISTRY_SIZE; i++) {
        if (registry[i] != NULL) {
            if (registry[i]->value) {
                free(registry[i]->value);
            }
            free(registry[i]);
            registry[i] = NULL;
            freed_count++;
        }
    }
    physical_node_count = 0;
    global_root = NULL; // Reset root for next use
    return PyLong_FromLong(freed_count);
}

// --- PYTHON API BRIDGE ---

static PyObject* method_insert(PyObject* self, PyObject* args) {
    unsigned long long h;
    char* val_str;
    if (!PyArg_ParseTuple(args, "Ks", &h, &val_str)) return NULL;

    if (!global_root) {
        // First run initialization
        for (int i = 0; i < REGISTRY_SIZE; i++) registry[i] = NULL;
        global_root = get_canonical(NULL, 0, NULL);
    }

    LQFTNode* path_nodes[MAX_BITS];
    uint32_t path_segs[MAX_BITS];
    int path_len = 0;

    LQFTNode* curr = global_root;
    int bit_depth = 0;

    // 1. Traversal
    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        path_nodes[path_len] = curr;
        path_segs[path_len] = segment;
        path_len++;

        if (curr->children[segment] == NULL) {
            curr = NULL;
            break;
        }
        curr = curr->children[segment];
        bit_depth += BIT_PARTITION;
    }

    // 2. Node Update
    LQFTNode* new_sub_node = NULL;
    if (curr == NULL) {
        new_sub_node = get_canonical(portable_strdup(val_str), h, NULL);
    } else if (curr->key_hash == h) {
        new_sub_node = get_canonical(portable_strdup(val_str), h, curr->children);
    } else {
        unsigned long long old_h = curr->key_hash;
        char* old_val = portable_strdup((char*)curr->value);
        int temp_depth = bit_depth;

        while (temp_depth < MAX_BITS) {
            uint32_t s_old = (old_h >> temp_depth) & MASK;
            uint32_t s_new = (h >> temp_depth) & MASK;

            if (s_old != s_new) {
                LQFTNode* c_old = get_canonical(old_val, old_h, curr->children);
                LQFTNode* c_new = get_canonical(portable_strdup(val_str), h, NULL);

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
        if (new_sub_node == NULL) {
            new_sub_node = get_canonical(portable_strdup(val_str), h, curr->children);
        }
    }

    // 3. Iterative Back-Propagation
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
    Py_RETURN_NONE;
}

static PyObject* method_search(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    if (!global_root) { Py_RETURN_NONE; }

    LQFTNode* curr = global_root;
    int bit_depth = 0;

    while (curr != NULL) {
        if (curr->value != NULL) {
            if (curr->key_hash == h) return PyUnicode_FromString((char*)curr->value);
            Py_RETURN_NONE;
        }
        uint32_t segment = (h >> bit_depth) & MASK;
        if (curr->children[segment] == NULL) { Py_RETURN_NONE; }
        curr = curr->children[segment];
        bit_depth += BIT_PARTITION;
        if (bit_depth >= MAX_BITS) break;
    }
    Py_RETURN_NONE;
}

static PyObject* method_get_metrics(PyObject* self, PyObject* args) {
    return Py_BuildValue("{s:i}", "physical_nodes", physical_node_count);
}

static PyMethodDef LQFTMethods[] = {
    {"insert", method_insert, METH_VARARGS, "Insert into C LQFT"},
    {"search", method_search, METH_VARARGS, "Search C LQFT"},
    {"get_metrics", method_get_metrics, METH_VARARGS, "Get memory metrics"},
    {"free_all", method_free_all, METH_VARARGS, "Clear all C memory"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef lqftmodule = {
    PyModuleDef_HEAD_INIT, "lqft_c_engine", "LQFT Performance Engine", -1, LQFTMethods
};

PyMODINIT_FUNC PyInit_lqft_c_engine(void) {
    return PyModule_Create(&lqftmodule);
}