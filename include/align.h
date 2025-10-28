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

#endif /* ALIGN_H */