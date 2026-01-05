#ifndef HNSW_H
#define HNSW_H

#include "bytes.h"
#include "vector.h"

typedef struct HNSWNode_ {
  Bytes *key;
  Vector *vec;

  int max_layer;
  struct HNSWNode_ ***adj; // 2D array
  int *adj_count;
} HNSWNode;

typedef struct HNSWIndex_ {
  HNSWNode *entry_point;
  int max_layer;
  int M;
  int ef_construction;
  DistanceMetric metric;
} HNSWIndex;

HNSWIndex *hnsw_create(DistanceMetric metric, int M, int ef_construction);
void hnsw_insert(HNSWIndex *index, const Bytes *key, Vector *v);
HNSWNode **hnsw_search(HNSWIndex *index, Vector *query, int k);

#endif // !HNSW_H
