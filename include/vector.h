#ifndef VECTOR_H
#define VECTOR_H

#include "bytes.h"
#include <stdint.h>

struct RObj;
typedef struct RObj r_obj;

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

r_obj *create_vector_object(Vector *v);


Vector *vector_create(uint32_t dimension, const float *init_data);
Vector *vector_dup(const Vector *v);
void vector_free(Vector *v);
Vector *create_random_vector(int dim);
void vector_normalize(Vector *v);
float vector_dist_l2(const Vector *v1, const Vector *v2);
float vector_dist_cosine(const Vector *v1, const Vector *v2);

#endif // !VECTOR_H
