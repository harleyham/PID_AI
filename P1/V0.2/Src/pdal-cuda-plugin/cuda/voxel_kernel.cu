#include <cuda_runtime.h>

#include <thrust/device_ptr.h>
#include <thrust/sort.h>
#include <thrust/copy.h>
#include <thrust/reduce.h>

#include <cstdint>
#include <cmath>
#include <stdexcept>
#include <string>
#include <vector>

namespace pdal
{

namespace
{

inline void cudaCheck(cudaError_t err, const char* msg)
{
    if (err != cudaSuccess)
        throw std::runtime_error(std::string(msg) + ": " + cudaGetErrorString(err));
}

__host__ __device__ inline uint64_t splitmix64(uint64_t x)
{
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}

__host__ __device__ inline uint64_t hashVoxel(int64_t ix, int64_t iy, int64_t iz)
{
    uint64_t hx = splitmix64(static_cast<uint64_t>(ix));
    uint64_t hy = splitmix64(static_cast<uint64_t>(iy));
    uint64_t hz = splitmix64(static_cast<uint64_t>(iz));

    return hx ^ (hy + 0x9e3779b97f4a7c15ULL + (hx << 6) + (hx >> 2)) ^
           (hz + 0x9e3779b97f4a7c15ULL + (hy << 6) + (hy >> 2));
}

__global__ void computeVoxelKeysKernel(
    const double* xyz,
    int n,
    double cell,
    uint64_t* keys,
    int* indices)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n)
        return;

    double x = xyz[3 * i + 0];
    double y = xyz[3 * i + 1];
    double z = xyz[3 * i + 2];

    int64_t ix = static_cast<int64_t>(floor(x / cell));
    int64_t iy = static_cast<int64_t>(floor(y / cell));
    int64_t iz = static_cast<int64_t>(floor(z / cell));

    keys[i] = hashVoxel(ix, iy, iz);
    indices[i] = i;
}

__global__ void markUniqueVoxelStartsKernel(
    const uint64_t* sortedKeys,
    int n,
    int* flags)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n)
        return;

    if (i == 0)
        flags[i] = 1;
    else
        flags[i] = (sortedKeys[i] != sortedKeys[i - 1]) ? 1 : 0;
}

struct IsOne
{
    __host__ __device__ bool operator()(const int x) const
    {
        return x == 1;
    }
};

} // unnamed namespace


std::vector<int> cudaVoxelSelect(const std::vector<double>& xyz, double cell)
{
    if (cell <= 0.0)
        throw std::runtime_error("filters.cudavoxel: parameter 'cell' must be > 0.");

    if (xyz.empty())
        return {};

    if (xyz.size() % 3 != 0)
        throw std::runtime_error("filters.cudavoxel: xyz vector size must be multiple of 3.");

    const int n = static_cast<int>(xyz.size() / 3);
    const size_t xyzBytes = xyz.size() * sizeof(double);
    const size_t keyBytes = static_cast<size_t>(n) * sizeof(uint64_t);
    const size_t intBytes = static_cast<size_t>(n) * sizeof(int);

    double* d_xyz = nullptr;
    uint64_t* d_keys = nullptr;
    int* d_indices = nullptr;
    int* d_flags = nullptr;
    int* d_outIndices = nullptr;

    try
    {
        cudaCheck(cudaMalloc(reinterpret_cast<void**>(&d_xyz), xyzBytes), "cudaMalloc d_xyz");
        cudaCheck(cudaMalloc(reinterpret_cast<void**>(&d_keys), keyBytes), "cudaMalloc d_keys");
        cudaCheck(cudaMalloc(reinterpret_cast<void**>(&d_indices), intBytes), "cudaMalloc d_indices");
        cudaCheck(cudaMalloc(reinterpret_cast<void**>(&d_flags), intBytes), "cudaMalloc d_flags");

        cudaCheck(cudaMemcpy(d_xyz, xyz.data(), xyzBytes, cudaMemcpyHostToDevice),
                  "cudaMemcpy H2D xyz");

        const int threads = 256;
        const int blocks = (n + threads - 1) / threads;

        computeVoxelKeysKernel<<<blocks, threads>>>(d_xyz, n, cell, d_keys, d_indices);
        cudaCheck(cudaGetLastError(), "computeVoxelKeysKernel launch");
        cudaCheck(cudaDeviceSynchronize(), "computeVoxelKeysKernel sync");

        thrust::device_ptr<uint64_t> keyPtr(d_keys);
        thrust::device_ptr<int> idxPtr(d_indices);
        thrust::device_ptr<int> flagPtr(d_flags);

        thrust::sort_by_key(keyPtr, keyPtr + n, idxPtr);

        markUniqueVoxelStartsKernel<<<blocks, threads>>>(d_keys, n, d_flags);
        cudaCheck(cudaGetLastError(), "markUniqueVoxelStartsKernel launch");
        cudaCheck(cudaDeviceSynchronize(), "markUniqueVoxelStartsKernel sync");

        const int kept = thrust::reduce(flagPtr, flagPtr + n, 0, thrust::plus<int>());

        std::vector<int> hostOut;
        hostOut.resize(kept);

        if (kept > 0)
        {
            cudaCheck(cudaMalloc(reinterpret_cast<void**>(&d_outIndices),
                                 static_cast<size_t>(kept) * sizeof(int)),
                      "cudaMalloc d_outIndices");

            thrust::device_ptr<int> outPtr(d_outIndices);

            thrust::copy_if(
                idxPtr, idxPtr + n,
                flagPtr,
                outPtr,
                IsOne());

            cudaCheck(cudaMemcpy(hostOut.data(), d_outIndices,
                                 static_cast<size_t>(kept) * sizeof(int),
                                 cudaMemcpyDeviceToHost),
                      "cudaMemcpy D2H outIndices");
        }

        cudaFree(d_outIndices);
        cudaFree(d_flags);
        cudaFree(d_indices);
        cudaFree(d_keys);
        cudaFree(d_xyz);

        return hostOut;
    }
    catch (...)
    {
        if (d_outIndices) cudaFree(d_outIndices);
        if (d_flags) cudaFree(d_flags);
        if (d_indices) cudaFree(d_indices);
        if (d_keys) cudaFree(d_keys);
        if (d_xyz) cudaFree(d_xyz);
        throw;
    }
}

} // namespace pdal
