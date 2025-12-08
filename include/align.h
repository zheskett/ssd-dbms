#ifndef ALIGN_H
#define ALIGN_H

#include <stdint.h>

/**
 * @brief Load a misaligned little-endian 32-bit unsigned integer from memory.
 *
 * @param p Pointer to the memory location.
 * @return The loaded value.
 */
uint32_t load_u32(const void* p);

/**
 * @brief Load a misaligned little-endian 64-bit unsigned integer from memory.
 *
 * @param p Pointer to the memory location.
 * @return The loaded value.
 */
uint64_t load_u64(const void* p);

/**
 * @brief Load a misaligned 8-bit unsigned integer from memory.
 *
 * @param p Pointer to the memory location.
 * @return The loaded value.
 */
uint8_t load_u8(const void* p);

/**
 * @brief Load a misaligned little-endian 32-bit float from memory.
 *
 * @param p Pointer to the memory location.
 * @return The loaded value.
 */
float load_f32(const void* p);

/**
 * @brief Load a misaligned little-endian 64-bit double from memory.
 *
 * @param p Pointer to the memory location.
 * @return The loaded value.
 */
double load_f64(const void* p);

/**
 * @brief Store an unsigned integer into misaligned memory.
 *
 * @param p Pointer to the memory location.
 * @param v The value to store.
 */
void store_u32(void* p, uint32_t v);

/**
 * @brief Store an unsigned 64-bit integer into misaligned memory.
 *
 * @param p Pointer to the memory location.
 * @param v The value to store.
 */
void store_u64(void* p, uint64_t v);

/**
 * @brief Store an unsigned 8-bit integer into misaligned memory.
 *
 * @param p Pointer to the memory location.
 * @param v The value to store.
 */
void store_u8(void* p, uint8_t v);

/**
 * @brief Store a 32-bit float into misaligned memory.
 *
 * @param p Pointer to the memory location.
 * @param f The value to store.
 */
void store_f32(void* p, float f);

/**
 * @brief Store a 64-bit double into misaligned memory.
 *
 * @param p Pointer to the memory location.
 * @param d The value to store.
 */
void store_f64(void* p, double d);

#endif /* ALIGN_H */
