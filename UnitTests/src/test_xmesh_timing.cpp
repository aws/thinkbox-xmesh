// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#include "stdafx.h"

#include <gtest/gtest.h>

#include <xmesh/xmesh_timing.hpp>

#include <frantic/files/filename_sequence.hpp>

using namespace frantic::files;
using namespace xmesh;

class xmesh_timing_test : public xmesh_timing {
  protected:
    double evaluate_playback_graph( double frame ) const { return frame; }
};

TEST( XMeshTiming, GetFrameVelocityOffset ) {
    xmesh_timing_test timing;

    frame_set frames;
    frames.add_frame( 0 );
    frames.add_frame( 0.5 );
    frames.add_frame( 1 );

    xmesh_timing::range_region rangeRegion;
    double sampleFrame;
    double sampleOffset;

    timing.get_frame_velocity_offset( 0.4, frames, rangeRegion, sampleFrame, sampleOffset );

    EXPECT_EQ( xmesh_timing::RANGE_INSIDE, rangeRegion );
    EXPECT_EQ( 0, sampleFrame );
    // Lower precision because the code uses float internally.
    // TODO: Consider changing to double internally, or change offset to use
    // float externally?
    EXPECT_FLOAT_EQ( 0.4f, static_cast<float>( sampleOffset ) );
}

TEST( XMeshTiming, GetSubframeVelocityOffset ) {
    xmesh_timing_test timing;

    frame_set frames;
    frames.add_frame( 0 );
    frames.add_frame( 0.5 );
    frames.add_frame( 1 );

    xmesh_timing::range_region rangeRegion;
    double sampleFrame;
    double sampleOffset;

    timing.get_subframe_velocity_offset( 0.4, frames, rangeRegion, sampleFrame, sampleOffset );

    EXPECT_EQ( xmesh_timing::RANGE_INSIDE, rangeRegion );
    EXPECT_EQ( 0.5, sampleFrame );
    EXPECT_FLOAT_EQ( -0.1f, static_cast<float>( sampleOffset ) );
}

TEST( XMeshTiming, GetFrameInterpolation ) {
    xmesh_timing_test timing;

    frame_set frames;
    frames.add_frame( 0 );
    frames.add_frame( 0.5 );
    frames.add_frame( 1 );

    xmesh_timing::range_region rangeRegion;
    std::pair<double, double> sampleFrames;
    double sampleOffset;

    timing.get_frame_interpolation( 0.4, frames, rangeRegion, sampleFrames, sampleOffset );

    EXPECT_EQ( xmesh_timing::RANGE_INSIDE, rangeRegion );
    EXPECT_EQ( std::make_pair( 0.0, 1.0 ), sampleFrames );
    EXPECT_FLOAT_EQ( 0.4f, static_cast<float>( sampleOffset ) );
}

TEST( XMeshTiming, GetSubframeInterpolation ) {
    xmesh_timing_test timing;

    frame_set frames;
    frames.add_frame( 0 );
    frames.add_frame( 0.5 );
    frames.add_frame( 1 );

    xmesh_timing::range_region rangeRegion;
    std::pair<double, double> sampleFrames;
    double sampleOffset;

    timing.get_subframe_interpolation( 0.4, frames, rangeRegion, sampleFrames, sampleOffset );

    EXPECT_EQ( xmesh_timing::RANGE_INSIDE, rangeRegion );
    EXPECT_EQ( std::make_pair( 0.0, 0.5 ), sampleFrames );
    EXPECT_FLOAT_EQ( 0.8f, static_cast<float>( sampleOffset ) );
}
