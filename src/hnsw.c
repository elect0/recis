#include "../include/hnsw.h"
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
  else
    return &node->adj[L + (2 * M) + ((layer - 1) * M)];

  return NULL;
}

static inline uint32_t get_neighbor_count(HNSWNode *node, int layer) {
  return node->adj[layer];
}

HNSWNode *hnsw_create_node(int L, int M, Vector *v, Bytes *key) {
  size_t data_slots = L + (2 * M) + ((L - 1) * M);
  size_t total_size = sizeof(HNSWNode) + (data_slots * sizeof(uint32_t));

  HNSWNode *node;
  if ((node = (HNSWNode *)malloc(total_size)) == NULL)
    return NULL;

  node->vec = v;
  node->key = bytes_dup(key);
  node->max_layer = L - 1;

  for (int i = 0; i < L; i++) {
    node->adj[i] = 0;
  }

  return node;
}

int hnsw_random_level(int M) {
  double P = 1.0 / log(M);
  double r = (double)rand() / RAND_MAX;

  int level = (int)(-log(r) * P);
  return level;
}

uint32_t hnsw_search_layer_greedy(HNSWIndex *index, Vector *query,
                                  uint32_t entry_id, int layer) {
  uint32_t curr_id = entry_id;
  float curr_dist = vector_dist_l2(query, index->nodes[curr_id]->vec);
  int changed = 1;

  while (changed) {
    changed = 0;
    uint32_t *neighbors = get_neighbor_list(index->nodes[curr_id], layer);
    uint32_t count = get_neighbor_count(index->nodes[curr_id], layer);

    for (uint32_t i = 0; i < count; i++) {
      uint32_t neighbor_id = neighbors[i];
      float d = vector_dist_l2(query, index->nodes[neighbor_id]->vec);
      if (d < curr_dist) {
        curr_dist = d;
        curr_id = neighbor_id;
        changed = 1;
      }
    }
  }

  return curr_id;
}

void candidate_list_insert(CandidateList *list, uint32_t node_id, float dist) {
  uint32_t i = 0;
  while (i < list->size && list->candidates[i].dist < dist) {
    i++;
  }

  if (i >= list->capacity) {
    return;
  }

  uint32_t elements_to_move = list->size - i;

  if (list->size == list->capacity) {
    elements_to_move = list->capacity - i - 1;
  }

  if (elements_to_move > 0) {
    memmove(&list->candidates[i + 1], &list->candidates[i],
            elements_to_move * sizeof(Candidate));
  }

  list->candidates[i].node_id = node_id;
  list->candidates[i].dist = dist;
  if (list->size < list->capacity) {
    list->size++;
  }
}

void bitset_set(uint8_t *bitset, uint32_t node_id) {
  uint32_t byte_idx = node_id >> 3;
  uint8_t bit_offset = node_id & 7;

  bitset[byte_idx] |= (1 << bit_offset);
}

int bitset_get(uint8_t *bitset, uint32_t node_id) {
  uint32_t byte_idx = node_id >> 3;
  uint8_t bit_offset = node_id & 7;

  return (bitset[byte_idx] & (1 << bit_offset)) != 0;
}

void bitset_clear(uint8_t *bitset, uint32_t node_id) {
  uint32_t byte_idx = node_id >> 3;
  uint8_t bit_offset = node_id & 7;

  bitset[byte_idx] &= ~(1 << bit_offset);
}

void visited_history_push(HNSWIndex *index, uint32_t node_id) {
  index->visited_history[index->visited_count++] = node_id;
}

void hnsw_search_layer_0(HNSWIndex *index, Vector *query, uint32_t entry_id,
                         CandidateList *results) {
  CandidateList candidates = {
      .candidates = malloc(index->ef_construction * sizeof(Candidate)),
      .capacity = index->ef_construction,
      .size = 0,
      .head = 0};

  float d = vector_dist_l2(query, index->nodes[entry_id]->vec);
  candidate_list_insert(&candidates, entry_id, d);
  candidate_list_insert(results, entry_id, d);

  bitset_set(index->visited_bitset, entry_id);
  visited_history_push(index, entry_id);

  while (candidates.head < candidates.size) {
    Candidate c = candidates.candidates[candidates.head++];

    Candidate further_res = results->candidates[results->size - 1];
    if (c.dist > further_res.dist && results->size >= results->capacity) {
      break;
    }

    uint32_t *neighbors = get_neighbor_list(index->nodes[entry_id], 0);
    uint32_t count = get_neighbor_count(index->nodes[entry_id], 0);

    for (uint32_t i = 0; i < count; i++) {
      uint32_t nid = neighbors[i];
      if (bitset_get(index->visited_bitset, nid) == 1)
        continue;

      bitset_set(index->visited_bitset, nid);
      visited_history_push(index, nid);

      float d = vector_dist_l2(query, index->nodes[nid]->vec);

      if (d < further_res.dist || results->size < results->capacity) {
        candidate_list_insert(&candidates, nid, d);
        candidate_list_insert(results, nid, d);
      }
    }
  }

  free(candidates.candidates);
}

HNSWIndex *hnsw_create(DistanceMetric metric, int M, int ef_construction) {
  HNSWIndex *index;
  if ((index = (HNSWIndex *)malloc(sizeof(HNSWIndex))) == NULL)
    return NULL;

  index->capacity = 1000;
  index->count = 0;
  index->nodes = calloc(index->capacity, sizeof(HNSWNode *));

  index->M = M;
  index->ef_construction = ef_construction;
  index->metric = metric;
  index->entry_point_id = -1;
  index->current_max_layer = -1;

  index->visited_bitset = calloc((index->capacity >> 3) + 1, sizeof(uint8_t));

  index->visited_history = malloc((ef_construction * M * sizeof(uint32_t)));
  index->visited_count = 0;

  return index;
}

void hnsw_insert(HNSWIndex *index, const Bytes *key, Vector *v) {
  uint32_t node_id = index->count++;
  int level = hnsw_random_level(index->M);

  HNSWNode *new_node = hnsw_create_node(level, index->M, v, (Bytes *)key);
  index->nodes[node_id] = new_node;

  if (index->entry_point_id == -1) {
    index->entry_point_id = node_id;
    index->current_max_layer = level;
    return;
  }

  uint32_t curr_entry_id = index->entry_point_id;

  for (int i = index->current_max_layer; i >= 0; i--) {
    if (i > level) {
      curr_entry_id = hnsw_search_layer_greedy(index, v, curr_entry_id, i);
    } else {
      CandidateList neighbors;
      neighbors.candidates = malloc(index->ef_construction * sizeof(Candidate));
      neighbors.capacity = index->ef_construction;
      neighbors.size = 0;
      neighbors.head = 0;

      hnsw_search_layer_0(index, v, curr_entry_id, &neighbors);
      uint32_t *my_adj = get_neighbor_list(new_node, i);
      uint32_t link_count =
          (neighbors.size > index->M) ? index->M : neighbors.size;

      for (int j = 0; j < link_count; j++) {
        my_adj[j] = neighbors.candidates[j].node_id;
      }

      new_node->adj[i] = link_count;

      for (int j = 0; j < link_count; j++) {
        uint32_t neighbor_id = neighbors.candidates[j].node_id;
        HNSWNode *nb_node = index->nodes[neighbor_id];
        uint32_t *nb_adj = get_neighbor_list(nb_node, i);

        uint32_t max = (i == 0) ? index->M * 2 : index->M;

        if (nb_node->adj[i] < max) {
          nb_adj[nb_node->adj[i]++] = node_id;
        }
      }
      curr_entry_id = neighbors.candidates[0].node_id;
      free(neighbors.candidates);
    }
  }

  if (level > index->current_max_layer) {
    index->entry_point_id = node_id;
    index->current_max_layer = level;
  }
}
