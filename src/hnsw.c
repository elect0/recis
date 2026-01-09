#include "../include/hnsw.h"

#include "../include/recis.h"
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static inline uint32_t *get_neighbor_list(HNSWNode *node, int layer) {
  if (layer > node->max_layer)
    return NULL;
  int L = node->max_layer + 1;
  int M = 16;
  if (layer == 0)
    return &node->adj[L];
  return &node->adj[L + (2 * M) + ((layer - 1) * M)];
}

static inline uint32_t get_neighbor_count(HNSWNode *node, int layer) {
  if (layer > node->max_layer)
    return 0;
  return node->adj[layer];
}

static inline float get_dist(HNSWIndex *index, const Vector *v1,
                             const Vector *v2) {
  if (index->metric == METRIC_L2)
    return vector_dist_l2(v1, v2);
  else
    return vector_dist_cosine(v1, v2);
}

r_obj *create_hnsw_object(DistanceMetric metric, int M, int ef_construction,
                          uint32_t dimension) {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL)
    return NULL;

  o->type = HNSW;
  o->data = (void *)hnsw_create(metric, M, ef_construction, dimension);

  return o;
}

void hnsw_free(HNSWIndex *index) {
  if (index == NULL)
    return;

  while (index->count > 0) {
    HNSWNode *node = index->nodes[index->count - 1];
    if (node) {
      if (node->key)
        free_bytes_object(node->key);

      if (node->vec)
        vector_free(node->vec);

      free(node);
    }
  }

  if (index->nodes)
    free(index->nodes);
  if (index->visited_history)
    free(index->visited_history);
  if (index->visited_bitset)
    free(index->visited_bitset);

  free(index);
}

HNSWNode *hnsw_create_node(int level, int M, Vector *v, Bytes *key) {
  int L = level;
  size_t data_slots = L + (2 * M) + ((L - 1) * M);
  if (L == 1)
    data_slots = 1 + (2 * M);

  size_t total_size = sizeof(HNSWNode) + (data_slots * sizeof(uint32_t));

  HNSWNode *node = (HNSWNode *)malloc(total_size);
  if (!node)
    return NULL;

  node->vec = vector_dup(v);
  node->key = bytes_dup(key);
  node->max_layer = level - 1;

  for (int i = 0; i < L; i++)
    node->adj[i] = 0;
  return node;
}

void bitset_set(uint8_t *bitset, uint32_t node_id) {
  bitset[node_id >> 3] |= (1 << (node_id & 7));
}

int bitset_get(uint8_t *bitset, uint32_t node_id) {
  return (bitset[node_id >> 3] & (1 << (node_id & 7))) != 0;
}

void bitset_clear(uint8_t *bitset, uint32_t node_id) {
  bitset[node_id >> 3] &= ~(1 << (node_id & 7));
}

void candidate_list_insert(CandidateList *list, uint32_t node_id, float dist) {
  uint32_t i = 0;
  while (i < list->size && list->candidates[i].dist < dist) {
    i++;
  }
  if (i >= list->capacity)
    return;

  uint32_t move_count = list->size - i;
  if (list->size == list->capacity)
    move_count = list->capacity - 1 - i;

  if (move_count > 0) {
    memmove(&list->candidates[i + 1], &list->candidates[i],
            move_count * sizeof(Candidate));
  }

  list->candidates[i].node_id = node_id;
  list->candidates[i].dist = dist;
  if (list->size < list->capacity)
    list->size++;
}

int hnsw_random_level(int M) {
  double r = (double)rand() / RAND_MAX;
  if (r < 1e-9)
    r = 1e-9;
  return (int)(-log(r) * (1.0 / log(M)));
}

HNSWIndex *hnsw_create(DistanceMetric metric, int M, int ef_construction,
                       uint32_t dimension) {
  HNSWIndex *index = malloc(sizeof(HNSWIndex));
  if (!index)
    return NULL;

  index->metric = metric;
  index->capacity = 1000;
  index->count = 0;
  index->nodes = calloc(index->capacity, sizeof(HNSWNode *));

  index->M = M;
  index->ef_construction = ef_construction;
  index->entry_point_id = -1;
  index->current_max_layer = -1;

  index->dimension = dimension;
  index->memory_used = 0;

  index->visited_bitset = calloc((index->capacity >> 3) + 1, sizeof(uint8_t));
  index->visited_history = malloc(ef_construction * M * 2 * sizeof(uint32_t));
  index->visited_count = 0;

  return index;
}

uint32_t hnsw_search_layer_greedy(HNSWIndex *index, Vector *query,
                                  uint32_t entry_id, int layer) {
  uint32_t curr_id = entry_id;
  float curr_dist = get_dist(index, query, index->nodes[curr_id]->vec);
  int changed = 1;

  while (changed) {
    changed = 0;
    uint32_t *neighbors = get_neighbor_list(index->nodes[curr_id], layer);
    uint32_t count = get_neighbor_count(index->nodes[curr_id], layer);

    uint32_t best_candidate = curr_id;
    float best_dist = curr_dist;

    for (uint32_t i = 0; i < count; i++) {
      uint32_t neighbor_id = neighbors[i];
      float d = get_dist(index, query, index->nodes[neighbor_id]->vec);

      if (d < best_dist) {
        best_dist = d;
        best_candidate = neighbor_id;
        changed = 1;
      }
    }

    if (changed) {
      curr_id = best_candidate;
      curr_dist = best_dist;
    }
  }
  return curr_id;
}

void hnsw_search_layer_base(HNSWIndex *index, Vector *query, uint32_t entry_id,
                            CandidateList *results, int layer) {
  CandidateList candidates = {
      .candidates = malloc(index->ef_construction * sizeof(Candidate)),
      .capacity = index->ef_construction,
      .size = 0,
      .head = 0};

  float d = get_dist(index, query, index->nodes[entry_id]->vec);
  candidate_list_insert(&candidates, entry_id, d);
  candidate_list_insert(results, entry_id, d);

  bitset_set(index->visited_bitset, entry_id);
  index->visited_history[index->visited_count++] = entry_id;

  while (candidates.size > 0) {
    // pop front
    Candidate c = candidates.candidates[0];
    if (candidates.size > 1) {
      memmove(candidates.candidates, candidates.candidates + 1,
              (candidates.size - 1) * sizeof(Candidate));
    }
    candidates.size--;

    Candidate furthest_res = results->candidates[results->size - 1];

    if (c.dist > furthest_res.dist && results->size >= results->capacity) {
      break;
    }

    uint32_t *neighbors = get_neighbor_list(index->nodes[c.node_id], layer);
    uint32_t count = get_neighbor_count(index->nodes[c.node_id], layer);

    for (uint32_t i = 0; i < count; i++) {
      uint32_t nid = neighbors[i];
      if (bitset_get(index->visited_bitset, nid))
        continue;

      bitset_set(index->visited_bitset, nid);
      index->visited_history[index->visited_count++] = nid;

      float dist = get_dist(index, query, index->nodes[nid]->vec);

      if (dist < furthest_res.dist || results->size < results->capacity) {
        candidate_list_insert(&candidates, nid, dist);
        candidate_list_insert(results, nid, dist);
      }
    }
  }
  free(candidates.candidates);
}

void hnsw_prune_add(uint32_t *neighbors, float *distances, uint32_t *count,
                    uint32_t max_m, uint32_t new_id, float new_dist) {
  int i = 0;
  while (i < *count && distances[i] < new_dist) {
    i++;
  }

  if (*count == max_m && i == max_m)
    return;

  uint32_t limit = (*count < max_m) ? *count : max_m - 1;
  if (limit > i) {
    memmove(&distances[i + 1], &distances[i], (limit - i) * sizeof(float));
    memmove(&neighbors[i + 1], &neighbors[i], (limit - i) * sizeof(uint32_t));
  }

  distances[i] = new_dist;
  neighbors[i] = new_id;
  if (*count < max_m)
    (*count)++;
}

void hnsw_insert(HNSWIndex *index, const Bytes *key, Vector *v) {
  size_t vector_memory = sizeof(Vector) + (index->dimension * sizeof(float));
  uint32_t key_len = key->length;

  if (index->metric == METRIC_COSINE) {
    vector_normalize(v);
  }

  if (index->count >= index->capacity) {
    uint32_t new_cap = index->capacity * 2;

    index->nodes = realloc(index->nodes, new_cap * sizeof(HNSWNode *));

    uint32_t old_bytes = (index->capacity >> 3) + 1;
    uint32_t new_bytes = (new_cap >> 3) + 1;
    index->visited_bitset = realloc(index->visited_bitset, new_bytes);
    memset(index->visited_bitset + old_bytes, 0, new_bytes - old_bytes);

    index->capacity = new_cap;
  }

  uint32_t node_id = index->count++;
  int level = hnsw_random_level(index->M);

  int links_count =
      (level == 0) ? (index->M * 2) : ((index->M) * 2) + (level * index->M);
  size_t node_mem = sizeof(HNSWNode) + (links_count * sizeof(uint32_t));

  HNSWNode *new_node = hnsw_create_node(level + 1, index->M, v, (Bytes *)key);
  index->nodes[node_id] = new_node;

  if (index->entry_point_id == -1) {
    index->entry_point_id = node_id;
    index->current_max_layer = level;

    index->memory_used += vector_memory + key_len + node_mem;
    return;
  }

  uint32_t curr_entry_id = index->entry_point_id;

  for (int i = index->current_max_layer; i >= 0; i--) {
    if (i > level) {
      curr_entry_id = hnsw_search_layer_greedy(index, v, curr_entry_id, i);
    } else {
      memset(index->visited_bitset, 0, (index->capacity >> 3) + 1);
      index->visited_count = 0;

      CandidateList neighbors;
      neighbors.candidates = malloc(index->ef_construction * sizeof(Candidate));
      neighbors.capacity = index->ef_construction;
      neighbors.size = 0;
      neighbors.head = 0;

      hnsw_search_layer_base(index, v, curr_entry_id, &neighbors, i);

      uint32_t *my_adj = get_neighbor_list(new_node, i);
      uint32_t max_links = (i == 0) ? index->M * 2 : index->M;
      uint32_t link_count =
          (neighbors.size > max_links) ? max_links : neighbors.size;

      for (int j = 0; j < link_count; j++) {
        my_adj[j] = neighbors.candidates[j].node_id;
      }
      new_node->adj[i] = link_count;

      for (int j = 0; j < link_count; j++) {
        uint32_t neighbor_id = neighbors.candidates[j].node_id;
        float dist_to_neighbor = neighbors.candidates[j].dist;

        HNSWNode *nb_node = index->nodes[neighbor_id];
        if (nb_node->max_layer < i)
          continue;

        uint32_t *nb_adj = get_neighbor_list(nb_node, i);
        uint32_t *nb_count = &nb_node->adj[i];
        uint32_t max = (i == 0) ? index->M * 2 : index->M;

        float temp_dists[256];

        for (int k = 0; k < *nb_count; k++) {
          temp_dists[k] =
              get_dist(index, nb_node->vec, index->nodes[nb_adj[k]]->vec);
        }

        hnsw_prune_add(nb_adj, temp_dists, nb_count, max, node_id,
                       dist_to_neighbor);
      }

      curr_entry_id = neighbors.candidates[0].node_id;
      free(neighbors.candidates);
    }
  }

  if (level > index->current_max_layer) {
    index->entry_point_id = node_id;
    index->current_max_layer = level;
  }

  index->memory_used += vector_memory + key_len + node_mem;
}

HNSWNode **hnsw_search(HNSWIndex *index, Vector *query, int k,
                       int *found_count) {

  if (index->metric == METRIC_COSINE) {
    vector_normalize(query);
  }

  memset(index->visited_bitset, 0, (index->capacity >> 3) + 1);
  index->visited_count = 0;

  uint32_t curr_entry = index->entry_point_id;

  for (int i = index->current_max_layer; i > 0; i--) {
    curr_entry = hnsw_search_layer_greedy(index, query, curr_entry, i);
  }

  CandidateList results;
  results.candidates = malloc(index->ef_construction * sizeof(Candidate));
  results.capacity = index->ef_construction;
  results.size = 0;
  results.head = 0;

  hnsw_search_layer_base(index, query, curr_entry, &results, 0);

  int result_limit = (results.size < k) ? results.size : k;
  *found_count = result_limit;

  HNSWNode **output_nodes = malloc(result_limit * sizeof(HNSWNode *));
  for (int i = 0; i < result_limit; i++) {
    output_nodes[i] = index->nodes[results.candidates[i].node_id];
  }

  free(results.candidates);
  return output_nodes;
}
