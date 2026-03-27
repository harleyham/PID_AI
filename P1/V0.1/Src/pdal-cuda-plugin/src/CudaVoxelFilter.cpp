#include "CudaVoxelFilter.hpp"

#include <pdal/PointView.hpp>
#include <pdal/PointTable.hpp>
#include <pdal/StageFactory.hpp>

#include <vector>

namespace pdal
{

static PluginInfo const s_info
{
    "filters.cudavoxel",
    "CUDA voxel downsampling filter",
    "https://pdal.org"
};

CREATE_SHARED_STAGE(CudaVoxelFilter, s_info)

std::string CudaVoxelFilter::getName() const
{
    return s_info.name;
}

void CudaVoxelFilter::addArgs(ProgramArgs& args)
{
    args.add("cell", "Voxel cell size", m_cell, 1.0);
}

std::vector<int> cudaVoxelSelect(const std::vector<double>& xyz, double cell);

PointViewSet CudaVoxelFilter::run(PointViewPtr view)
{
    std::vector<double> xyz;
    xyz.reserve(view->size() * 3);

    for (PointId i = 0; i < view->size(); ++i)
    {
        xyz.push_back(view->getFieldAs<double>(Dimension::Id::X, i));
        xyz.push_back(view->getFieldAs<double>(Dimension::Id::Y, i));
        xyz.push_back(view->getFieldAs<double>(Dimension::Id::Z, i));
    }

    std::vector<int> keep = cudaVoxelSelect(xyz, m_cell);

    PointViewPtr out(new PointView(view->table()));
    for (int idx : keep)
        out->appendPoint(*view, static_cast<PointId>(idx));

    PointViewSet s;
    s.insert(out);
    return s;
}

} // namespace pdal
