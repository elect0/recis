#ifndef HNSW_H
#define HNSW_H

#include "bytes.h"
#include "vector.h"
#include <stdint.h>

typedef struct HNSWNode_ {
  Bytes *key;
  Vector *vec;

  int max_layer;
  uint32_t adj[];
} HNSWNode;

typedef struct HNSWIndex_ {
  HNSWNode **nodes;
  uint32_t count;
  uint32_t capacity;

  int entry_point_id;
  int current_max_layer;

  int M;
  int ef_construction;
  DistanceMetric metric;

  uint32_t *visited_history;
  uint32_t visited_count;

  uint8_t *visited_bitset;
} HNSWIndex;

typedef struct Candidate_ {
  uint32_t node_id;
  float dist;
} Candidate;

typedef struct CandidateList_ {
  Candidate *candidates;
  uint16_t size;
  uint16_t capacity;
  uint16_t head;
} CandidateList;

HNSWNode *hnsw_create_node(int L, int M, Vector *v, Bytes *key);
HNSWIndex *hnsw_create(DistanceMetric metric, int M, int ef_construction);
void hnsw_insert(HNSWIndex *index, const Bytes *key, Vector *v);
int hnsw_random_level(int M);
uint32_t hnsw_search_layer_greedy(HNSWIndex *index, Vector *query,
                                  uint32_t entry_id, int layer);
void hnsw_search_layer_0(HNSWIndex *index, Vector *query, uint32_t entry_id, CandidateList *results);

#endif // !HNSW_H
