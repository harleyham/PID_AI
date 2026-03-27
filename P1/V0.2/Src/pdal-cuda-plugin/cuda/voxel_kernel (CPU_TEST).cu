#include <vector>
#include <cmath>
#include <cstdint>
#include <unordered_set>
#include <functional>

namespace pdal
{

struct VoxelKey
{
    int64_t ix, iy, iz;

    bool operator==(const VoxelKey& other) const
    {
        return ix == other.ix && iy == other.iy && iz == other.iz;
    }
};

struct VoxelKeyHash
{
    std::size_t operator()(const VoxelKey& k) const
    {
        std::size_t h1 = std::hash<int64_t>{}(k.ix);
        std::size_t h2 = std::hash<int64_t>{}(k.iy);
        std::size_t h3 = std::hash<int64_t>{}(k.iz);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

std::vector<int> cudaVoxelSelect(const std::vector<double>& xyz, double cell)
{
    std::unordered_set<VoxelKey, VoxelKeyHash> seen;
    std::vector<int> keep;

    const int n = static_cast<int>(xyz.size() / 3);
    keep.reserve(n);

    for (int i = 0; i < n; ++i)
    {
        double x = xyz[3 * i + 0];
        double y = xyz[3 * i + 1];
        double z = xyz[3 * i + 2];

        VoxelKey key{
            static_cast<int64_t>(std::floor(x / cell)),
            static_cast<int64_t>(std::floor(y / cell)),
            static_cast<int64_t>(std::floor(z / cell))
        };

        if (seen.insert(key).second)
            keep.push_back(i);
    }

    return keep;
}

} // namespace pdal
