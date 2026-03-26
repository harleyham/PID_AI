#pragma once

#include <pdal/Filter.hpp>
#include <pdal/PointView.hpp>
#include <pdal/util/ProgramArgs.hpp>

namespace pdal
{

class CudaVoxelFilter : public Filter
{
public:
    CudaVoxelFilter() = default;
    ~CudaVoxelFilter() override = default;

    std::string getName() const override;

private:
    double m_cell = 1.0;

    void addArgs(ProgramArgs& args) override;
    PointViewSet run(PointViewPtr view) override;
};

} // namespace pdal
