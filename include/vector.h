#ifndef VECTOR_H
#define VECTOR_H

#include "bytes.h"
typedef enum DistnaceMetric_ {
  METRIC_L2,
  METRIC_COSINE,
} DistanceMetric;

typedef struct Vector_ {
  uint32_t dimension;
  uint32_t flags;
  float data[];
} Vector;

typedef struct VSResult_ {
  Bytes *key;
  double distance;
} VSResult;

Vector *vector_create(uint32_t dimension, const float *init_data);
void vector_free(Vector *v);

float vector_dist_l2(const Vector *v1, const Vector *v2);
float vector_dist_cosine(const Vector *v1, const Vector *v2);

float vector_dot_product(const Vector *v1, const Vector *v2);

#endif // !VECTOR_H
