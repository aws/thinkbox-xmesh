// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#include <frantic/strings/tstring.hpp>

namespace frantic {
namespace files {

class frame_set;

}
} // namespace frantic

namespace xmesh {

class xmesh_timing {
  public:
    xmesh_timing();
    virtual ~xmesh_timing();

    void set_offset( double frameOffset );
    void set_range( double startFrame, double endFrame );

    // for error messages
    void set_sequence_name( const frantic::tstring& sequenceName );

    enum range_region { RANGE_BEFORE = -1, RANGE_INSIDE = 0, RANGE_AFTER = 1 };

    void get_frame_velocity_offset( double frame, const frantic::files::frame_set& frameSet, range_region& rangeRegion,
                                    double& sampleFrame, double& sampleOffset ) const;
    void get_subframe_velocity_offset( double frame, const frantic::files::frame_set& frameSet,
                                       range_region& rangeRegion, double& sampleFrame, double& sampleOffset ) const;
    double get_time_derivative( double frame, double frameStep ) const;

    void get_frame_interpolation( double frame, const frantic::files::frame_set& frameSet, range_region& rangeRegion,
                                  std::pair<double, double>& sampleFrames, double& sampleOffset ) const;
    void get_subframe_interpolation( double frame, const frantic::files::frame_set& frameSet, range_region& rangeRegion,
                                     std::pair<double, double>& sampleFrames, double& sampleOffset ) const;

  protected:
    virtual double evaluate_playback_graph( double frame ) const = 0;

    const frantic::tstring& get_sequence_name() const;

  private:
    struct xmesh_extrapolation_info {
        range_region rangeRegion;
        double frame;
        double offset;
    };

    struct xmesh_interpolation_info {
        range_region rangeRegion;
        std::pair<double, double> frames;
        double alpha;
    };

    xmesh_extrapolation_info get_extrapolation_info( double outFrame, const frantic::files::frame_set& frameSet,
                                                     bool useConsistentSubframeSamples ) const;
    xmesh_interpolation_info get_interpolation_info( double frame, const frantic::files::frame_set& frameSet,
                                                     bool useSubframes ) const;

    double get_timing_data_without_limit_to_range( double frame ) const;

    double m_frameOffset;

    bool m_limitToRange;
    std::pair<double, double> m_frameRange;

    frantic::tstring m_sequenceName;
};

} // namespace xmesh
