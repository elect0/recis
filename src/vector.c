#include "../include/vector.h"
#include <immintrin.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <xmmintrin.h>

static inline float hsum256_ps(__m256 v) {
  __m128 lo = _mm256_castps256_ps128(v);
  __m128 hi = _mm256_extractf128_ps(v, 1);
  __m128 sum128 = _mm_add_ps(lo, hi);

  __m128 shuf = _mm_movehdup_ps(sum128);
  __m128 sums = _mm_add_ps(sum128, shuf);
  shuf = _mm_movehl_ps(shuf, sums);
  sums = _mm_add_ps(sums, shuf);
  return _mm_cvtss_f32(sums);
}

Vector *vector_create(uint32_t dimension, const float *init_data) {

  void *ptr;
  if (posix_memalign(&ptr, 32,
                     sizeof(Vector) + (sizeof(float) * dimension) + 32) != 0)
    return NULL;

  Vector *v = (Vector *)ptr;

  v->dimension = dimension;
  v->flags = 0;

  if (init_data != NULL) {
    memcpy(v->data, init_data, sizeof(float) * dimension);
  } else {
    memset(v->data, 0, sizeof(float) * dimension);
  }

  return v;
}

void vector_free(Vector *v) {
  if (v != NULL)
    free(v);
}

float vector_dist_l2(const Vector *v1, const Vector *v2) {
  __m256 sum_vec = _mm256_setzero_ps();
  uint32_t i = 0;

  for (; i <= v1->dimension - 8; i += 8) {
    __m256 va = _mm256_load_ps(&v1->data[i]);
    __m256 vb = _mm256_load_ps(&v2->data[i]);
    __m256 diff = _mm256_sub_ps(va, vb);
    sum_vec = _mm256_fmadd_ps(diff, diff, sum_vec);
  }

  float final_sum = hsum256_ps(sum_vec);

  for (; i < v1->dimension; i++) {
    float diff = v1->data[i] - v2->data[i];
    final_sum += diff * diff;
  }

  return final_sum;
}

void vector_normalize(Vector *v) {
  float dot = 0.0f;

  for (uint32_t i = 0; i < v->dimension; i++) {
    dot += v->data[i] * v->data[i];
  }

  float mag = sqrtf(dot);
  if (mag < 1e-9)
    return; // Avoid division by 0

  float inv_mag = 1.0f / mag;
  for (uint32_t i = 0; i < v->dimension; i++) {
    v->data[i] *= inv_mag;
  }
}

float vector_dist_cosine(const Vector *v1, const Vector *v2) {
  __m256 sum_vec = _mm256_setzero_ps();
  uint32_t i = 0;

  for (; i < v1->dimension - 8; i += 8) {
    __m256 va = _mm256_load_ps(&v1->data[i]);
    __m256 vb = _mm256_load_ps(&v2->data[i]);
    sum_vec = _mm256_fmadd_ps(va, vb, sum_vec);
  }

  float final_dot = hsum256_ps(sum_vec);
  for (; i < v1->dimension; i++) {
    final_dot += v1->data[i] * v2->data[i];
  }

  return final_dot;
}
