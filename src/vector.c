#include "../include/vector.h"
#include "../include/recis.h"

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

r_obj *create_vector_object(uint32_t dimension, const float *init_data) {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL)
    return NULL;

  o->type = VECTOR;
  o->data = (void *)create_vector_object(dimension, init_data);

  return o;
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
  float sum = 0;
  int i = 0;

#ifdef __AVX__
  __m256 sum256 = _mm256_setzero_ps();
  for (; i <= v1->dimension - 8; i += 8) {
    __m256 a = _mm256_loadu_ps(v1->data + i);
    __m256 b = _mm256_loadu_ps(v2->data + i);
    __m256 diff = _mm256_sub_ps(a, b);
    sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(diff, diff));
  }
  sum = hsum256_ps(sum256);
#endif

  for (; i < v1->dimension; i++) {
    float diff = v1->data[i] - v2->data[i];
    sum += diff * diff;
  }
  return sqrtf(sum);
}

void vector_normalize(Vector *v) {
  float dot = 0.0f;
  uint32_t i = 0;

  // Calculate magnitude
#ifdef __AVX__
  __m256 sum256 = _mm256_setzero_ps();
  for (; i <= v->dimension - 8; i += 8) {
    __m256 a = _mm256_loadu_ps(v->data + i);
    sum256 = _mm256_fmadd_ps(a, a, sum256);
  }
  dot = hsum256_ps(sum256);
#endif
  for (; i < v->dimension; i++) {
    dot += v->data[i] * v->data[i];
  }

  float mag = sqrtf(dot);
  if (mag < 1e-9)
    return; // Avoid division by 0

  float inv_mag = 1.0f / mag;

  i = 0;

  // Divide each element by magnitude
#ifdef __AVX__
  __m256 inv_mag256 = _mm256_set1_ps(inv_mag);
  for (; i <= v->dimension - 8; i += 8) {
    __m256 a = _mm256_loadu_ps(v->data + i);
    _mm256_storeu_ps(v->data + i, _mm256_mul_ps(a, inv_mag256));
  }
#endif
  for (; i < v->dimension; i++) {
    v->data[i] *= inv_mag;
  }
}

float vector_dist_cosine(const Vector *v1, const Vector *v2) {

  float dot = 0;
  int i = 0;
#ifdef __AVX__
  __m256 sum256 = _mm256_setzero_ps();
  for (; i <= v1->dimension - 8; i += 8) {
    __m256 a = _mm256_loadu_ps(v1->data + i);
    __m256 b = _mm256_loadu_ps(v2->data + i);
    sum256 = _mm256_fmadd_ps(a, b, sum256);
  }
  dot = hsum256_ps(sum256);
#endif

  for (; i < v1->dimension; i++) {
    dot += v1->data[i] * v2->data[i];
  }
  return 1.0f - dot;
}

Vector *create_random_vector(int dim) {
  float *temp_data = malloc(dim * sizeof(float));
  for (int i = 0; i < dim; i++) {
    temp_data[i] = ((float)rand() / RAND_MAX);
  }

  Vector *v = vector_create(dim, temp_data);

  free(temp_data);
  return v;
}
