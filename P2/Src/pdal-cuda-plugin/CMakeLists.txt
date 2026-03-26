cmake_minimum_required(VERSION 3.24)
project(pdal_cuda_plugin LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CUDA_RUNTIME_LIBRARY Shared)

find_package(PDAL REQUIRED CONFIG)

include_directories(include)

PDAL_CREATE_PLUGIN(
    TYPE filter
    NAME cudavoxel
    VERSION 1.0
    SOURCES
        src/CudaVoxelFilter.cpp
        cuda/voxel_kernel.cu
)

target_include_directories(pdal_plugin_filter_cudavoxel
    PRIVATE
        include
)

set_target_properties(pdal_plugin_filter_cudavoxel PROPERTIES
    CUDA_SEPARABLE_COMPILATION ON
    CUDA_RESOLVE_DEVICE_SYMBOLS ON
    POSITION_INDEPENDENT_CODE ON
)
