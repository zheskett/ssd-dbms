#include "align.h"

#include <string.h>

uint32_t load_u32(const void* p) {
  uint32_t v;
  memcpy(&v, p, sizeof v);
  return v;
}

uint64_t load_u64(const void* p) {
  uint64_t v;
  memcpy(&v, p, sizeof v);
  return v;
}

uint8_t load_u8(const void* p) {
  uint8_t v;
  memcpy(&v, p, sizeof v);
  return v;
}

float load_f32(const void* p) {
  float f;
  memcpy(&f, p, sizeof f);
  return f;
}

double load_f64(const void* p) {
  double d;
  memcpy(&d, p, sizeof d);
  return d;
}

void store_u32(void* p, uint32_t v) {
  memcpy(p, &v, sizeof v);
}

void store_u64(void* p, uint64_t v) {
  memcpy(p, &v, sizeof v);
}

void store_u8(void* p, uint8_t v) {
  memcpy(p, &v, sizeof v);
}

void store_f32(void* p, float f) {
  memcpy(p, &f, sizeof f);
}

void store_f64(void* p, double d) {
  memcpy(p, &d, sizeof d);
}
