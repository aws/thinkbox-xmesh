// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdlib>
#include <limits>

#include <frantic/math/utils.hpp>

namespace xmesh {

/**
 *  Iterate over a fraction of the indices within a range [0..count).
 * The returned indices are approximately evenly distributed within the range.
 */
class fractional_index_iterator {
    std::size_t m_index;
    std::size_t m_acc;
    std::size_t m_count;
    std::size_t m_fractionalCount;

    void find_valid_index() {
        for( ; m_index < m_count; ++m_index ) {
            m_acc += m_fractionalCount;
            if( m_acc >= m_count ) {
                m_acc -= m_count;
                return;
            }
        }

        m_index = std::numeric_limits<std::size_t>::max();
    }

  public:
    fractional_index_iterator( std::size_t count, float fraction )
        : m_index( 0 )
        , m_acc( 0 )
        , m_count( count )
        , m_fractionalCount(
              frantic::math::clamp<std::size_t>( static_cast<std::size_t>( fraction * count ), 0, count ) ) {
        find_valid_index();
    }

    fractional_index_iterator()
        : m_index( std::numeric_limits<std::size_t>::max() ) {}

    std::size_t operator*() const { return m_index; }

    bool operator==( const fractional_index_iterator& other ) const { return m_index == other.m_index; }

    bool operator!=( const fractional_index_iterator& other ) const { return m_index != other.m_index; }

    fractional_index_iterator& operator++() {
        if( m_index != std::numeric_limits<std::size_t>::max() ) {
            ++m_index;
            find_valid_index();
        }
        return *this;
    }

    std::size_t num_indices() const { return m_fractionalCount; }
};

} // namespace xmesh
