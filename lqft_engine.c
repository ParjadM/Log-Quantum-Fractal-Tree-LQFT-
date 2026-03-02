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
 * LQFT C-Engine - V4.5 (Full CRUD & Large Payload)
 * Architect: Parjad Minooei
 * * CHANGE LOG:
 * - Added 'delete' method with bottom-up path reconstruction.
 * - Maintains Incremental FNV-1a Hashing for large payloads.
 * - Fixed Stack-to-Heap collision for multi-MB data.
 */

#define BIT_PARTITION 5
#define MAX_BITS 64 
#define MASK 0x1F 
#define REGISTRY_SIZE 8000009 

typedef struct LQFTNode {
    void* value;
    uint64_t key_hash;
    struct LQFTNode* children[32]; 
    char struct_hash[17]; 
} LQFTNode;

static LQFTNode** registry = NULL;
static int physical_node_count = 0;
static LQFTNode* global_root = NULL;

const uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
const uint64_t FNV_PRIME = 1099511628211ULL;

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
    for (int i = 0; i < 32; i++) node->children[i] = NULL;
    return node;
}

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

    while (registry[idx] != NULL) {
        if (strcmp(registry[idx]->struct_hash, lookup_hash) == 0) {
            if (value) free(value); 
            return registry[idx];
        }
        idx = (idx + 1) % REGISTRY_SIZE;
        if (idx == start_idx) break; 
    }

    LQFTNode* new_node = create_node(value, key_hash);
    if (!new_node) return NULL;
    if (children) {
        for (int i = 0; i < 32; i++) new_node->children[i] = children[i];
    }
    strcpy(new_node->struct_hash, lookup_hash);
    registry[idx] = new_node;
    physical_node_count++;
    return new_node;
}

static PyObject* method_delete(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    if (!global_root) Py_RETURN_NONE;

    LQFTNode* path_nodes[20]; 
    uint32_t path_segs[20];
    int path_len = 0;
    LQFTNode* curr = global_root;
    int bit_depth = 0;

    // Trace path to target
    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        path_nodes[path_len] = curr;
        path_segs[path_len] = segment;
        path_len++;
        curr = curr->children[segment];
        bit_depth += BIT_PARTITION;
    }

    // If not found, exit
    if (curr == NULL || curr->key_hash != h) Py_RETURN_NONE;

    // Rebuild bottom-up: start with NULL (the deleted leaf)
    LQFTNode* new_sub_node = NULL;

    for (int i = path_len - 1; i >= 0; i--) {
        LQFTNode* p_node = path_nodes[i];
        uint32_t segment = path_segs[i];
        LQFTNode* new_children[32];
        int has_other_children = 0;

        for (int j = 0; j < 32; j++) {
            if (j == segment) new_children[j] = new_sub_node;
            else {
                new_children[j] = p_node->children[j];
                if (new_children[j]) has_other_children = 1;
            }
        }

        if (!has_other_children && i > 0) {
            new_sub_node = NULL; // Contract empty branch
        } else {
            new_sub_node = get_canonical(NULL, 0, new_children);
        }
    }

    global_root = (new_sub_node) ? new_sub_node : get_canonical(NULL, 0, NULL);
    Py_RETURN_NONE;
}

static PyObject* method_free_all(PyObject* self, PyObject* args) {
    if (registry != NULL) {
        for (int i = 0; i < REGISTRY_SIZE; i++) {
            if (registry[i] != NULL) {
                if (registry[i]->value) free(registry[i]->value);
                free(registry[i]);
                registry[i] = NULL;
            }
        }
        free(registry);
        registry = NULL;
    }
    physical_node_count = 0;
    global_root = NULL;
    Py_RETURN_NONE;
}

static PyObject* method_insert(PyObject* self, PyObject* args) {
    unsigned long long h;
    char* val_str;
    if (!PyArg_ParseTuple(args, "Ks", &h, &val_str)) return NULL;
    if (!global_root) {
        if (!init_registry()) return PyErr_NoMemory();
        global_root = get_canonical(NULL, 0, NULL);
    }
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
    if (curr == NULL) { new_sub_node = get_canonical(portable_strdup(val_str), h, NULL); }
    else if (curr->key_hash == h) { new_sub_node = get_canonical(portable_strdup(val_str), h, curr->children); }
    else {
        unsigned long long old_h = curr->key_hash;
        char* old_val = portable_strdup((char*)curr->value);
        int temp_depth = bit_depth;
        while (temp_depth < 64) {
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
            } else { path_nodes[path_len] = NULL; path_segs[path_len] = s_old; path_len++; temp_depth += BIT_PARTITION; }
        }
        if (new_sub_node == NULL) new_sub_node = get_canonical(portable_strdup(val_str), h, curr->children);
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
    Py_RETURN_NONE;
}

static PyObject* method_search(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    if (!global_root) Py_RETURN_NONE;
    LQFTNode* curr = global_root;
    int bit_depth = 0;
    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        curr = curr->children[segment];
        bit_depth += BIT_PARTITION;
    }
    if (curr != NULL && curr->key_hash == h) return PyUnicode_FromString((char*)curr->value);
    Py_RETURN_NONE;
}

static PyObject* method_get_metrics(PyObject* self, PyObject* args) {
    return Py_BuildValue("{s:i}", "physical_nodes", physical_node_count);
}

static PyMethodDef LQFTMethods[] = {
    {"insert", method_insert, METH_VARARGS, "Insert key-value"},
    {"delete", method_delete, METH_VARARGS, "Delete key"},
    {"search", method_search, METH_VARARGS, "Search key"},
    {"get_metrics", method_get_metrics, METH_VARARGS, "Get engine stats"},
    {"free_all", method_free_all, METH_VARARGS, "Total memory wipe"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef lqftmodule = { PyModuleDef_HEAD_INIT, "lqft_c_engine", NULL, -1, LQFTMethods };

PyMODINIT_FUNC PyInit_lqft_c_engine(void) { return PyModule_Create(&lqftmodule); }