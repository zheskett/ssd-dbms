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