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
 * LQFT C-Engine - V4.3 (Dynamic Registry Build)
 * Architect: Parjad Minooei
 * * BUGFIX: Added missing 'search' method to the native export table.
 * * MEMORY: Moved Registry to HEAP (Dynamic) for zero-footprint reclamation.
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

// Registry is now a pointer allocated on the HEAP to allow full OS reclamation
static LQFTNode** registry = NULL;
static int physical_node_count = 0;
static LQFTNode* global_root = NULL;

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

static int init_registry() {
    if (registry == NULL) {
        // Use calloc to initialize all pointer slots to NULL safely
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

    char buffer[8192] = { 0 };
    if (value != NULL) {
        sprintf(buffer, "leaf:%s:%llu", (char*)value, (unsigned long long)key_hash);
    } else {
        sprintf(buffer, "branch:");
        for (int i = 0; i < 32; i++) {
            if (children && children[i]) {
                char seg_buf[32];
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

static PyObject* method_free_all(PyObject* self, PyObject* args) {
    int freed_count = 0;
    if (registry != NULL) {
        for (int i = 0; i < REGISTRY_SIZE; i++) {
            if (registry[i] != NULL) {
                if (registry[i]->value) free(registry[i]->value);
                free(registry[i]);
                registry[i] = NULL;
                freed_count++;
            }
        }
        // Dynamic reclamation: Free the registry array itself
        free(registry);
        registry = NULL;
    }
    physical_node_count = 0;
    global_root = NULL;
    return PyLong_FromLong(freed_count);
}

static PyObject* method_insert(PyObject* self, PyObject* args) {
    unsigned long long h;
    char* val_str;
    if (!PyArg_ParseTuple(args, "Ks", &h, &val_str)) return NULL;

    if (!global_root) {
        if (!init_registry()) return PyErr_NoMemory();
        global_root = get_canonical(NULL, 0, NULL);
    }

    LQFTNode* path_nodes[MAX_BITS];
    uint32_t path_segs[MAX_BITS];
    int path_len = 0;

    LQFTNode* curr = global_root;
    int bit_depth = 0;

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

    if (!global_root || registry == NULL) {
        Py_RETURN_NONE;
    }

    LQFTNode* curr = global_root;
    int bit_depth = 0;

    while (curr != NULL && curr->value == NULL) {
        uint32_t segment = (h >> bit_depth) & MASK;
        curr = curr->children[segment];
        bit_depth += BIT_PARTITION;
    }

    if (curr != NULL && curr->key_hash == h) {
        return PyUnicode_FromString((char*)curr->value);
    }

    Py_RETURN_NONE;
}

static PyObject* method_get_metrics(PyObject* self, PyObject* args) {
    return Py_BuildValue("{s:i}", "physical_nodes", physical_node_count);
}

static PyMethodDef LQFTMethods[] = {
    {"insert", method_insert, METH_VARARGS, "Insert"},
    {"search", method_search, METH_VARARGS, "Search"},
    {"get_metrics", method_get_metrics, METH_VARARGS, "Metrics"},
    {"free_all", method_free_all, METH_VARARGS, "Reclaim"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef lqftmodule = { PyModuleDef_HEAD_INIT, "lqft_c_engine", NULL, -1, LQFTMethods };

PyMODINIT_FUNC PyInit_lqft_c_engine(void) { return PyModule_Create(&lqftmodule); }