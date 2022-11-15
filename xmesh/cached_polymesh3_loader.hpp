// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <boost/array.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>

#include <frantic/geometry/polymesh3.hpp>
#include <frantic/geometry/xmesh_reader.hpp>

#define FRANTIC_POLYMESH_COPY_ON_WRITE

namespace xmesh {

namespace LOAD_POLYMESH3_MASK {
enum load_polymesh3_mask {
    VERTS = 1,
    FACES = 2,
    VELOCITY = 4,
    MAPS = 8,
    BOX = 16,
    //
    STATIC_MESH = VERTS + FACES + MAPS + BOX,
    ALL = VERTS + FACES + VELOCITY + MAPS + BOX
};
};

class polymesh3_loader_interface {
  public:
    virtual frantic::geometry::const_polymesh3_ptr
    load( const frantic::tstring& filename, frantic::geometry::xmesh_metadata* outMetadata, int loadMask ) = 0;
    virtual void set_thread_count( std::size_t numThreads ) = 0;
    virtual void clear_cache() = 0;
};

#ifdef FRANTIC_POLYMESH_COPY_ON_WRITE

struct xmesh_cache_entry {
    boost::filesystem::path::string_type filename;
    frantic::geometry::xmesh_reader reader;
    frantic::geometry::xmesh_metadata metadata;

    xmesh_cache_entry( const boost::filesystem::path::string_type& path )
        : reader( path ) {}
};

struct polymesh3_cache_data_entry {
    frantic::geometry::polymesh3_channel_data data;

    polymesh3_cache_data_entry() {}

    polymesh3_cache_data_entry( frantic::geometry::polymesh3_channel_data data )
        : data( data ) {}
};

struct polymesh3_cache_face_entry {
    frantic::geometry::polymesh3_channel_faces faces;
    frantic::geometry::polymesh3_channel_faces facesEnd;
    std::size_t numFaces;

    polymesh3_cache_face_entry()
        : numFaces( 0 ) {}

    polymesh3_cache_face_entry( std::vector<int>& facesIn )
        : numFaces( 0 )
        , faces( facesIn ) {}
};

struct polymesh3_cache {
    typedef boost::unordered_map<boost::filesystem::path::string_type, polymesh3_cache_data_entry> data_cache_t;
    typedef boost::unordered_map<boost::filesystem::path::string_type, polymesh3_cache_face_entry> faces_cache_t;

    data_cache_t data;
    faces_cache_t faces;
};

class cached_polymesh3_loader : public polymesh3_loader_interface {
  public:
    cached_polymesh3_loader();
    frantic::geometry::const_polymesh3_ptr load( const frantic::tstring& filename,
                                                 frantic::geometry::xmesh_metadata* outMetadata, int loadMask );
    void set_thread_count( std::size_t numThreads );
    void clear_cache();

  private:
    polymesh3_cache m_cache;
    boost::array<boost::shared_ptr<xmesh_cache_entry>, 2> m_xmeshCache;
    std::size_t m_threadCount;

    // return the filename's index in m_xmeshCache, or -1 if it is not cached
    int find_xmesh_cache_entry_index( const boost::filesystem::path::string_type& filename );

    frantic::geometry::const_polymesh3_ptr
    build_polymesh3_from_cache( frantic::geometry::xmesh_reader& reader, polymesh3_cache& cache,
                                std::vector<frantic::tstring>& vertexChannelNames,
                                std::vector<frantic::tstring>& faceChannelNames, bool loadVerts, bool loadFaces,
                                const frantic::tstring& filename );

    frantic::geometry::const_polymesh3_ptr load_xmesh( const frantic::tstring& filename,
                                                       frantic::geometry::xmesh_metadata* outMetadata, int loadMask );

    xmesh_cache_entry& get_xmesh_cache_entry( const boost::filesystem::path::string_type& filename );
};

#else

class cached_polymesh3_loader : public polymesh3_loader_interface {
  public:
    cached_polymesh3_loader();
    frantic::geometry::const_polymesh3_ptr load( const frantic::tstring& filename,
                                                 frantic::geometry::xmesh_metadata* outMetadata, int loadMask );
    void set_thread_count( std::size_t numThreads );
    void clear_cache();

  private:
    typedef boost::mutex mutex_t;

    mutex_t m_cachedMeshMutex;
    frantic::tstring m_cachedMeshFilename;
    frantic::geometry::xmesh_metadata m_cachedMeshMetadata;
    frantic::geometry::polymesh3_ptr m_cachedMesh;
    int m_cachedMask;
};

#endif

} // namespace xmesh
