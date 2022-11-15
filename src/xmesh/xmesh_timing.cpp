// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#include "stdafx.h"

#ifndef _BOOL
#define _BOOL
#endif

#include <xmesh/xmesh_timing.hpp>

#include <frantic/files/filename_sequence.hpp>
#include <frantic/math/utils.hpp>

using namespace xmesh;

namespace {

bool get_nearest_subframe( const frantic::files::frame_set& frameSet, double frame, double* pOutFrame,
                           const bool useWholeFrames = false ) {
    std::pair<double, double> range;
    float alpha;
    double outFrame;

    if( frameSet.empty() ) {
        return false;
    }

    if( useWholeFrames ) {
        if( frameSet.get_nearest_wholeframe_interval( frame, range, alpha ) ) {
            if( alpha < 0.5f ) {
                outFrame = range.first;
            } else {
                outFrame = range.second;
            }
        } else {
            return false;
        }
    } else {
        if( frameSet.frame_exists( frame ) ) {
            outFrame = frame;
        } else if( frameSet.get_nearest_subframe_interval( frame, range, alpha ) ) {
            if( alpha < 0.5f ) {
                outFrame = range.first;
            } else {
                outFrame = range.second;
            }
        } else {
            return false;
        }
    }

    if( pOutFrame ) {
        *pOutFrame = outFrame;
    }

    return true;
}

} // anonymous namespace

xmesh_timing::xmesh_timing()
    : m_frameOffset( 0 )
    , m_limitToRange( false )
    , m_frameRange( 0, 0 ) {}

xmesh_timing::~xmesh_timing() {}

void xmesh_timing::set_offset( double frameOffset ) { m_frameOffset = frameOffset; }

void xmesh_timing::set_range( double startFrame, double endFrame ) {
    if( !( startFrame <= endFrame ) ) {
        throw std::runtime_error( "set_range Error: startFrame must be less than or equal to endFrame" );
    }
    m_limitToRange = true;
    m_frameRange = std::pair<double, double>( startFrame, endFrame );
}

void xmesh_timing::set_sequence_name( const frantic::tstring& sequenceName ) { m_sequenceName = sequenceName; }

/*
double xmesh_timing::evaluate_playback_graph( double frame ) const{
        return frame;
}
*/

void xmesh_timing::get_frame_velocity_offset( double frame, const frantic::files::frame_set& frameSet,
                                              range_region& rangeInterval, double& sampleFrame,
                                              double& sampleOffset ) const {
    xmesh_extrapolation_info info = get_extrapolation_info( frame, frameSet, true );
    rangeInterval = info.rangeRegion;
    sampleFrame = info.frame;
    sampleOffset = info.offset;
}

void xmesh_timing::get_subframe_velocity_offset( double frame, const frantic::files::frame_set& frameSet,
                                                 range_region& rangeInterval, double& sampleFrame,
                                                 double& sampleOffset ) const {
    xmesh_extrapolation_info info = get_extrapolation_info( frame, frameSet, false );
    rangeInterval = info.rangeRegion;
    sampleFrame = info.frame;
    sampleOffset = info.offset;
}

double xmesh_timing::get_time_derivative( double frame, double frameStep ) const {
    if( frameStep == 0 ) {
        throw std::runtime_error( "Frame step cannot be zero when computing time derivative." );
    }

    double timeDerivative;

    if( m_limitToRange ) {
        const double evalFrame = get_timing_data_without_limit_to_range( frame );
        const double evalWholeFrame = get_timing_data_without_limit_to_range(
            frantic::math::round( frame ) ); // Note:  not necessarily a whole frame after retiming

        // To match get_extrapolation_info, make sure both evalFrame and evalWholeFrame in range.    (Corresponds to
        // inFrame and inWholeFrame.)
        if( evalFrame >= m_frameRange.first && evalFrame <= m_frameRange.second &&
            evalWholeFrame >= m_frameRange.first && evalWholeFrame <= m_frameRange.second ) {
            double intervalStart = get_timing_data_without_limit_to_range( frame - ( frameStep / 2 ) );
            double intervalEnd = get_timing_data_without_limit_to_range( frame + ( frameStep / 2 ) );
            timeDerivative = ( intervalEnd - intervalStart ) / frameStep;
        } else {
            timeDerivative = 0;
        }
    } else {
        double intervalStart = get_timing_data_without_limit_to_range( frame - ( frameStep / 2 ) );
        double intervalEnd = get_timing_data_without_limit_to_range( frame + ( frameStep / 2 ) );
        timeDerivative = ( intervalEnd - intervalStart ) / frameStep;
    }

    return timeDerivative;
}

void xmesh_timing::get_frame_interpolation( double frame, const frantic::files::frame_set& frameSet,
                                            range_region& rangeInterval, std::pair<double, double>& sampleFrames,
                                            double& alpha ) const {
    xmesh_interpolation_info info = get_interpolation_info( frame, frameSet, false );
    rangeInterval = info.rangeRegion;
    sampleFrames = info.frames;
    alpha = info.alpha;
}

void xmesh_timing::get_subframe_interpolation( double frame, const frantic::files::frame_set& frameSet,
                                               range_region& rangeInterval, std::pair<double, double>& sampleFrames,
                                               double& alpha ) const {
    xmesh_interpolation_info info = get_interpolation_info( frame, frameSet, true );
    rangeInterval = info.rangeRegion;
    sampleFrames = info.frames;
    alpha = info.alpha;
}

const frantic::tstring& xmesh_timing::get_sequence_name() const { return m_sequenceName; }

xmesh_timing::xmesh_extrapolation_info xmesh_timing::get_extrapolation_info( double outFrame,
                                                                             const frantic::files::frame_set& frameSet,
                                                                             bool useConsistentSubframeSamples ) const {
    // "out" times correspond to times in the viewport or render, before the frameOffset etc. are applied
    // "in" times correspond to times in the frameSet, after frameOffset etc. are applied

    if( frameSet.empty() ) {
        throw std::runtime_error( "No files found in sequence: \"" +
                                  frantic::strings::to_string( get_sequence_name() ) + "\"" );
    }

    const double outWholeFrame = frantic::math::round( outFrame );

    const double inFrame = get_timing_data_without_limit_to_range( outFrame );

    // The input frame corresponding to the outWholeFrame.
    // This is not necessarily a whole frame!
    const double inWholeFrame = get_timing_data_without_limit_to_range( outWholeFrame );

    range_region rangeRegion = RANGE_INSIDE;

    // look for a time close to this in the frameSet
    double requestFrame = inFrame;
    if( m_limitToRange && inFrame < m_frameRange.first ) {
        if( inWholeFrame < m_frameRange.first ) {
            rangeRegion = RANGE_BEFORE;
            requestFrame = m_frameRange.first;
        } else {
            requestFrame = inWholeFrame;
        }
    } else if( m_limitToRange && inFrame > m_frameRange.second ) {
        if( inWholeFrame > m_frameRange.second ) {
            rangeRegion = RANGE_AFTER;
            requestFrame = m_frameRange.second;
        } else {
            requestFrame = inWholeFrame;
        }
    } else {
        if( useConsistentSubframeSamples ) {
            requestFrame = inWholeFrame;
        } else {
            requestFrame = inFrame;
        }
    }

    // a time close to requestFrame in the frameSet
    double inSampleFrame;
    bool foundSampleFrame = get_nearest_subframe( frameSet, requestFrame, &inSampleFrame );
    if( !foundSampleFrame ) {
        throw std::runtime_error(
            "An appropriate frame to offset for time " + boost::lexical_cast<std::string>( inWholeFrame ) +
            " could not be found in the sequence: \"" + frantic::strings::to_string( get_sequence_name() ) + "\"" );
    }

    xmesh_timing::xmesh_extrapolation_info result;
    result.rangeRegion = rangeRegion;
    result.frame = inSampleFrame;
    result.offset = ( rangeRegion == RANGE_INSIDE ? ( inFrame - inSampleFrame ) : 0 );
    return result;
}

xmesh_timing::xmesh_interpolation_info xmesh_timing::get_interpolation_info( double frame,
                                                                             const frantic::files::frame_set& frameSet,
                                                                             bool useSubframes ) const {
    if( frameSet.empty() ) {
        throw std::runtime_error( "No files found in sequence: \"" +
                                  frantic::strings::to_string( get_sequence_name() ) + "\"" );
    }

    double inFrame = get_timing_data_without_limit_to_range( frame );
    range_region rangeRegion = RANGE_INSIDE;

    if( m_limitToRange ) {
        if( inFrame < m_frameRange.first ) {
            rangeRegion = RANGE_BEFORE;
        } else if( inFrame > m_frameRange.second ) {
            rangeRegion = RANGE_AFTER;
        }
        inFrame = frantic::math::clamp( inFrame, m_frameRange.first, m_frameRange.second );
    }

    std::pair<double, double> frameBracket;
    float alpha;
    bool foundFrames;
    if( useSubframes ) {
        foundFrames = frameSet.get_nearest_subframe_interval( inFrame, frameBracket, alpha );
    } else {
        foundFrames = frameSet.get_nearest_wholeframe_interval( inFrame, frameBracket, alpha );
    }
    if( !foundFrames ) {
        throw std::runtime_error(
            "Appropriate frames to interpolate at time " + boost::lexical_cast<std::string>( frame ) +
            " could not be found in the sequence: \"" + frantic::strings::to_string( get_sequence_name() ) + "\"" );
    }

    xmesh_timing::xmesh_interpolation_info result;
    result.rangeRegion = rangeRegion;
    result.frames = frameBracket;
    result.alpha = alpha;
    return result;
}

double xmesh_timing::get_timing_data_without_limit_to_range( double frame ) const {
    frame = evaluate_playback_graph( frame );

    frame += m_frameOffset;

    return frame;
}
