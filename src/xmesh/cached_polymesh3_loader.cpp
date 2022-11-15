// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#include "stdafx.h"

#include <xmesh/cached_polymesh3_loader.hpp>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include <boost/timer.hpp>
#include <boost/tuple/tuple.hpp>

#ifndef FRANTIC_DISABLE_THREADS
#ifdef TBB_AVAILABLE
#include <tbb/parallel_for.h>
#include <tbb/blocked_range.h>
#include <tbb/pipeline.h>
#include <tbb/task_scheduler_init.h>
#else
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#endif
#endif

#include <frantic/files/paths.hpp>
#include <frantic/geometry/const_shared_polymesh3_builder.hpp>
#include <frantic/geometry/polymesh3_builder.hpp>
#include <frantic/geometry/polymesh3_file_io.hpp>
#include <frantic/geometry/xmesh_reader.hpp>
#include <frantic/threads/synchronizedqueue.hpp>

using namespace frantic::geometry;
using namespace xmesh;

#ifdef FRANTIC_POLYMESH_COPY_ON_WRITE

namespace {

#if !defined( FRANTIC_DISABLE_THREADS ) && !defined( TBB_AVAILABLE )

#pragma message( "Using boost threads" )

// A simple thread pool for saving xmesh channels in parallel.
class thread_pool {
    boost::asio::io_service m_service;
    boost::shared_ptr<boost::asio::io_service::work> m_work;
    boost::thread_group m_threads;
    boost::thread* m_thread;

  public:
    thread_pool( std::size_t threadCount )
        : m_service( std::max<std::size_t>( threadCount, 1 ) )
        , m_work( new boost::asio::io_service::work( m_service ) ) {
        threadCount = std::max<std::size_t>( threadCount, 1 );
        for( std::size_t i = 0; i < threadCount; ++i ) {
            m_thread = m_threads.create_thread( boost::bind( &boost::asio::io_service::run, &m_service ) );
        }
    }

    ~thread_pool() {
        m_work.reset();
        m_threads.join_all();
    }

    template <typename F>
    void schedule( F task ) {
        m_service.post( task );
    }
};

#elif !defined( FRANTIC_DISABLE_THREADS ) && defined( TBB_AVAILABLE )

#pragma message( "Using TBB threads" )

class enumerate_functions : public tbb::filter {
  public:
    enumerate_functions( std::vector<boost::function<void( void )>>& functions )
        : tbb::filter( tbb::filter::serial )
        , m_index( 0 )
        , m_functions( functions ) {}

    void* operator()( void* ) {
        if( m_index == m_functions.size() ) {
            return 0;
        }

        void* result = &m_functions[m_index];
        ++m_index;
        return result;
    }

  private:
    std::size_t m_index;
    std::vector<boost::function<void( void )>>& m_functions;
};

class invoke_functions : public tbb::filter {
  public:
    invoke_functions()
        : tbb::filter( tbb::filter::parallel ) {}

    void* operator()( void* item ) {
        if( item ) {
            boost::function<void( void )>* f = reinterpret_cast<boost::function<void( void )>*>( item );
            ( *f )();
        }
        return 0;
    }
};

#else

#pragma message( "Threads are disabled" )

class thread_pool {
  public:
    thread_pool( std::size_t /*threadCount*/ ) {}

    template <typename F>
    void schedule( F task ) {
        task();
    }
};

#endif

void parallel_invoke_functions( std::vector<boost::function<void( void )>>& functions, std::size_t threadCount ) {
#if !defined( FRANTIC_DISABLE_THREADS ) && defined( TBB_AVAILABLE )
    tbb::task_scheduler_init taskScheduler;

    tbb::pipeline pipeline;

    enumerate_functions enumerateFunctions( functions );
    invoke_functions invokeFunctions;

    pipeline.add_filter( enumerateFunctions );
    pipeline.add_filter( invokeFunctions );

    pipeline.run( std::max<int>( 1, static_cast<int>( threadCount ) ) );
#else
    thread_pool threadpool( std::max<int>( 1, static_cast<int>( threadCount ) ) );

    BOOST_FOREACH( boost::function<void( void )>& f, functions ) {
        threadpool.schedule( f );
    }
#endif
}

} // anonymous namespace

cached_polymesh3_loader::cached_polymesh3_loader() {
    // const int maxThreads = 4;
    // set_thread_count( std::max<std::size_t>( 1, static_cast<std::size_t>( std::min<int>( maxThreads,
    // boost::thread::hardware_concurrency() ) ) ) );
    set_thread_count( 1 );
}

typedef frantic::threads::SynchronizedQueue<std::string> error_queue_t;

class xmesh_consistency_error : public std::runtime_error {
  public:
    xmesh_consistency_error( const std::string& message )
        : std::runtime_error( message ) {}
};

static void load_vertex_channel( const xmesh_reader& reader, const frantic::tstring& name, char* pData,
                                 frantic::channels::data_type_t expectedType, std::size_t expectedArity,
                                 std::size_t expectedCount, error_queue_t& errorQueue ) {
    std::string errMsg;
    try {
        reader.load_vertex_channel( name, pData, expectedType, expectedArity, expectedCount );
    } catch( std::exception& e ) {
        errMsg = e.what();
    } catch( ... ) {
        errMsg = "load_vertex_channel Error: An unknown exception occurred.";
    }
    if( !errMsg.empty() ) {
        errorQueue.enter( errMsg );
    }
}

static void load_face_channel( const xmesh_reader& reader, const frantic::tstring& name, char* pData,
                               frantic::channels::data_type_t expectedType, std::size_t expectedArity,
                               std::size_t expectedCount, error_queue_t& errorQueue ) {
    std::string errMsg;
    try {
        reader.load_face_channel( name, pData, expectedType, expectedArity, expectedCount );
    } catch( std::exception& e ) {
        errMsg = e.what();
    } catch( ... ) {
        errMsg = "load_face_channel Error: An unknown exception occurred.";
    }
    if( !errMsg.empty() ) {
        errorQueue.enter( errMsg );
    }
}

static void load_vertex_channel_faces_cache( const xmesh_reader& reader, const frantic::tstring& name,
                                             polymesh3_cache_face_entry& cache, std::size_t expectedCount,
                                             error_queue_t& errorQueue ) {
    std::string errMsg;
    try {
        std::vector<int> faceIndexBuffer( expectedCount );
        if( expectedCount > 0 ) {
            reader.load_vertex_channel_faces( name, reinterpret_cast<char*>( &faceIndexBuffer[0] ), expectedCount );
        }
        std::size_t numFaces = 0;
        std::vector<int> facesEndIndexBuffer;
        facesEndIndexBuffer.resize( 0 );
        facesEndIndexBuffer.reserve( 5000 );
        int* geomFaceIndexBuffer = expectedCount > 0 ? &faceIndexBuffer[0] : 0;
        for( std::size_t i = 0; i < expectedCount; ++i ) {
            if( geomFaceIndexBuffer[i] < 0 ) {
                geomFaceIndexBuffer[i] = -geomFaceIndexBuffer[i] - 1;
                facesEndIndexBuffer.push_back( (int)( i + 1 ) );
                ++numFaces;
            }
        }
        polymesh3_channel_faces channelFaces( faceIndexBuffer );
        polymesh3_channel_faces channelFacesEnd( facesEndIndexBuffer );
        cache.faces = channelFaces;
        cache.facesEnd = channelFacesEnd;
        cache.numFaces = numFaces;
    } catch( std::exception& e ) {
        errMsg = e.what();
    } catch( ... ) {
        errMsg = "load_vertex_channel_faces_cache Error: An unknown exception occurred.";
    }
    if( !errMsg.empty() ) {
        errorQueue.enter( errMsg );
    }
}

static void assert_expected_cache_data( const polymesh3_cache& cache,
                                        const boost::filesystem::path::string_type& filename,
                                        frantic::channels::data_type_t expectedType, std::size_t expectedArity,
                                        std::size_t expectedCount ) {
    polymesh3_cache::data_cache_t::const_iterator it = cache.data.find( filename );
    if( it == cache.data.end() ) {
        throw std::runtime_error( "assert_expected_cache_data: Missing cache entry for file \"" +
                                  frantic::strings::to_string( filename ) + "\"" );
    }
    const polymesh3_cache_data_entry& cacheEntry = it->second;
    if( !cacheEntry.data.is_valid() ) {
        throw std::runtime_error( "assert_expected_cache_data: Cache entry for file \"" +
                                  frantic::strings::to_string( filename ) + "\" is NULL" );
    }
    if( cacheEntry.data.type() != expectedType ) {
        throw xmesh_consistency_error(
            "assert_expected_cache_data: File \"" + frantic::strings::to_string( filename ) +
            "\" has the wrong data type.  Expected " +
            frantic::strings::to_string( frantic::channels::channel_data_type_str( expectedType ) ) + " but got " +
            frantic::strings::to_string( frantic::channels::channel_data_type_str( cacheEntry.data.type() ) ) +
            " instead." );
    }
    if( cacheEntry.data.arity() != expectedArity ) {
        throw xmesh_consistency_error( "assert_expected_cache_data: File \"" + frantic::strings::to_string( filename ) +
                                       "\" has the wrong arity.  Expected " +
                                       boost::lexical_cast<std::string>( expectedArity ) + " but got " +
                                       boost::lexical_cast<std::string>( cacheEntry.data.arity() ) + " instead." );
    }
    const std::size_t primitiveSize =
        frantic::channels::sizeof_channel_data_type( cacheEntry.data.type() ) * cacheEntry.data.arity();
    if( primitiveSize == 0 ) {
        throw xmesh_consistency_error( "assert_expected_cache_data: File \"" + frantic::strings::to_string( filename ) +
                                       "\" has primitive size 0." );
    }
    if( cacheEntry.data.element_size() == 0 ) {
        throw xmesh_consistency_error( "assert_expected_cache_data: File \"" + frantic::strings::to_string( filename ) +
                                       "\" has element size 0." );
    }
    const std::size_t count = cacheEntry.data.get().size() / primitiveSize;
    if( count != expectedCount ) {
        throw xmesh_consistency_error( "assert_expected_cache_data: File \"" + frantic::strings::to_string( filename ) +
                                       "\" has the data count.  Expected " +
                                       boost::lexical_cast<std::string>( expectedCount ) + " but got " +
                                       boost::lexical_cast<std::string>( count ) + " instead." );
    }
}

static void assert_expected_cache_faces( const polymesh3_cache& cache,
                                         const boost::filesystem::path::string_type& filename,
                                         std::size_t expectedCount ) {
    polymesh3_cache::faces_cache_t::const_iterator it = cache.faces.find( filename );
    if( it == cache.faces.end() ) {
        throw std::runtime_error( "assert_expected_cache_faces: Missing faces cache entry for file \"" +
                                  frantic::strings::to_string( filename ) + "\"" );
    }
    const polymesh3_cache_face_entry& cacheEntry = it->second;
    if( !cacheEntry.faces.is_valid() ) {
        throw std::runtime_error( "assert_expected_cache_faces: Faces cache entry for file \"" +
                                  frantic::strings::to_string( filename ) + "\" is NULL" );
    }
    if( !cacheEntry.facesEnd.is_valid() ) {
        throw std::runtime_error( "assert_expected_cache_faces: Faces end cache entry for file \"" +
                                  frantic::strings::to_string( filename ) + "\" is NULL" );
    }
    if( cacheEntry.faces.get().size() != expectedCount ) {
        throw xmesh_consistency_error(
            "assert_expected_cache_faces: File \"" + frantic::strings::to_string( filename ) +
            "\" has incorrect data count.  Expected " + boost::lexical_cast<std::string>( expectedCount ) +
            " but got " + boost::lexical_cast<std::string>( cacheEntry.faces.get().size() ) + " instead." );
    }
}

static void get_keep_channel_refs( xmesh_reader& reader, polymesh3_cache& cache,
                                   const std::vector<frantic::tstring>& vertexChannelNames, bool loadVertexChannelFaces,
                                   const std::vector<frantic::tstring>& faceChannelNames,
                                   std::vector<polymesh3_channel_data>& keepData,
                                   std::vector<polymesh3_channel_faces>& keepFaces ) {
    BOOST_FOREACH( const frantic::tstring& vertexChannelName, vertexChannelNames ) {
        const xmesh_vertex_channel& ch = reader.get_vertex_channel( vertexChannelName );
        std::size_t numChannelFaceElements = ch.get_face_count();

        {
            polymesh3_cache::data_cache_t::iterator i = cache.data.find( ch.get_vertex_file_path().native() );
            if( i != cache.data.end() ) {
                keepData.push_back( i->second.data );
            }
        }

        if( loadVertexChannelFaces && numChannelFaceElements > 0 ) {
            polymesh3_cache::faces_cache_t::iterator i = cache.faces.find( ch.get_face_file_path().native() );
            if( i != cache.faces.end() ) {
                keepFaces.push_back( i->second.faces );
            }
        }
    }

    BOOST_FOREACH( const frantic::tstring& faceChannelName, faceChannelNames ) {
        const xmesh_face_channel& ch = reader.get_face_channel( faceChannelName );

        polymesh3_cache::data_cache_t::iterator i = cache.data.find( ch.get_face_file_path().native() );
        if( i != cache.data.end() ) {
            keepData.push_back( i->second.data );
        }
    }
}

void load_missing_files( const frantic::tstring& filenameForErrorMessage, xmesh_reader& reader,
                         const polymesh3_cache& cache, const std::vector<frantic::tstring>& vertexChannelNames,
                         bool loadVertexChannelFaces, const std::vector<frantic::tstring>& faceChannelNames,
                         std::size_t threadCount,
                         std::map<boost::filesystem::path::string_type, polymesh3_cache_data_entry>& missingData,
                         std::map<boost::filesystem::path::string_type, polymesh3_cache_face_entry>& missingFaces ) {
    using namespace frantic::channels;
    using boost::tie;

    typedef std::map<boost::filesystem::path::string_type, polymesh3_cache_data_entry> missing_data_t;
    typedef std::map<boost::filesystem::path::string_type, polymesh3_cache_face_entry> missing_faces_t;

    const xmesh_vertex_channel& geomCh = reader.get_vertex_channel( _T("verts") );
    const std::size_t numVerts = geomCh.get_vertex_count();
    const std::size_t numFaceElements = geomCh.get_face_count();

    error_queue_t errorQueue;
    { // scope for loadFunctions
        std::vector<boost::function<void( void )>> loadFunctions;

        bool didInsert = false;

        BOOST_FOREACH( const frantic::tstring& vertexChannelName, vertexChannelNames ) {
            const xmesh_vertex_channel& ch = reader.get_vertex_channel( vertexChannelName );
            const std::size_t numChannelVerts = ch.get_vertex_count();
            const std::size_t numChannelFaceElements = ch.get_face_count();
            const std::pair<frantic::channels::data_type_t, std::size_t> typeInfo = ch.get_vertex_type();

            if( numChannelFaceElements > 0 ) {
                if( numChannelFaceElements != numFaceElements )
                    throw std::runtime_error(
                        "load_polymesh_file() The channel \"" + frantic::strings::to_string( vertexChannelName ) +
                        "\" did not have the same custom face layout as the geometry channel in file \"" +
                        frantic::strings::to_string( filenameForErrorMessage ) + "\"" );
            } else {
                if( numChannelVerts != numVerts )
                    throw std::runtime_error(
                        "load_polymesh_file() The channel \"" + frantic::strings::to_string( vertexChannelName ) +
                        "\" did not have the same vertex layout as the geometry channel in file \"" +
                        frantic::strings::to_string( filenameForErrorMessage ) + "\"" );
            }

            { // scope for data
                polymesh3_cache::data_cache_t::const_iterator j = cache.data.find( ch.get_vertex_file_path().native() );
                if( j == cache.data.end() ) {
                    missing_data_t::iterator i = missingData.find( ch.get_vertex_file_path().native() );
                    if( i == missingData.end() ) {
                        {
                            frantic::graphics::raw_byte_buffer buffer;
                            buffer.resize( frantic::channels::sizeof_channel_data_type( typeInfo.first ) *
                                           typeInfo.second * numChannelVerts );
                            polymesh3_channel_data channelData( buffer, typeInfo.first, typeInfo.second );
                            boost::tuples::tie( i, didInsert ) =
                                missingData.insert( polymesh3_cache::data_cache_t::value_type(
                                    ch.get_vertex_file_path().native(), channelData ) );
                        }
                        loadFunctions.push_back(
                            boost::bind( load_vertex_channel, boost::ref( reader ), vertexChannelName,
                                         i->second.data.get_writable().ptr_at( 0 ), typeInfo.first, typeInfo.second,
                                         numChannelVerts, boost::ref( errorQueue ) ) );
                    }
                }
            }

            if( loadVertexChannelFaces && numChannelFaceElements > 0 ) { // scope for faces
                polymesh3_cache::faces_cache_t::const_iterator j = cache.faces.find( ch.get_face_file_path().native() );
                if( j == cache.faces.end() ) {
                    missing_faces_t::iterator i = missingFaces.find( ch.get_face_file_path().native() );
                    if( i == missingFaces.end() ) {
                        std::vector<int> buffer( numChannelFaceElements );
                        boost::tuples::tie( i, didInsert ) =
                            missingFaces.insert( polymesh3_cache::faces_cache_t::value_type(
                                ch.get_face_file_path().native(), polymesh3_cache_face_entry( buffer ) ) );
                        loadFunctions.push_back( boost::bind( load_vertex_channel_faces_cache, boost::ref( reader ),
                                                              vertexChannelName, boost::ref( i->second ),
                                                              numChannelFaceElements, boost::ref( errorQueue ) ) );
                    }
                }
            }
        }

        BOOST_FOREACH( const frantic::tstring& faceChannelName, faceChannelNames ) {
            const xmesh_face_channel& ch = reader.get_face_channel( faceChannelName );
            const std::size_t numChannelFaces = ch.get_face_count();
            const std::pair<frantic::channels::data_type_t, std::size_t> typeInfo = ch.get_face_type();

            polymesh3_cache::data_cache_t::const_iterator j = cache.data.find( ch.get_face_file_path().native() );
            if( j == cache.data.end() ) {
                missing_data_t::iterator i = missingData.find( ch.get_face_file_path().native() );
                if( i == missingData.end() ) {
                    {
                        frantic::graphics::raw_byte_buffer buffer;
                        buffer.resize( frantic::channels::sizeof_channel_data_type( typeInfo.first ) * typeInfo.second *
                                       numChannelFaces );
                        polymesh3_channel_data channelData( buffer, typeInfo.first, typeInfo.second );
                        boost::tuples::tie( i, didInsert ) =
                            missingData.insert( polymesh3_cache::data_cache_t::value_type(
                                ch.get_face_file_path().native(), channelData ) );
                    }
                    if( !didInsert ) {
                        throw std::runtime_error( "Internal error: no insert performed for new cache entry" );
                    }
                    loadFunctions.push_back( boost::bind( load_face_channel, boost::ref( reader ), faceChannelName,
                                                          i->second.data.get_writable().ptr_at( 0 ), typeInfo.first,
                                                          typeInfo.second, numChannelFaces,
                                                          boost::ref( errorQueue ) ) );
                }
            }
        }

        parallel_invoke_functions( loadFunctions, threadCount );
    }

    // Check for thread errors before we proceed
    if( errorQueue.size() ) {
        std::string errMsg;
        bool success = errorQueue.leave( errMsg );
        if( success ) {
            throw std::runtime_error( errMsg );
        } else {
            throw std::runtime_error( "An unknown error occurred while attempting to retrieve worker error message." );
        }
    }
}

void cache_missing_files( const frantic::tstring& filenameForErrorMessage, xmesh_reader& reader, polymesh3_cache& cache,
                          const std::vector<frantic::tstring>& vertexChannels, bool loadVertexChannelFaces,
                          const std::vector<frantic::tstring>& faceChannels, std::size_t threadCount ) {
    typedef std::map<boost::filesystem::path::string_type, polymesh3_cache_data_entry> missing_data_t;
    missing_data_t missingData;
    typedef std::map<boost::filesystem::path::string_type, polymesh3_cache_face_entry> missing_faces_t;
    missing_faces_t missingFaces;

    load_missing_files( filenameForErrorMessage, reader, cache, vertexChannels, loadVertexChannelFaces, faceChannels,
                        threadCount, missingData, missingFaces );

    // insert after loading is finished,
    // so we don't insert if an exception occurred during load
    for( missing_data_t::iterator i = missingData.begin(); i != missingData.end(); ++i ) {
        cache.data[i->first] = i->second;
    }
    for( missing_faces_t::iterator i = missingFaces.begin(); i != missingFaces.end(); ++i ) {
        cache.faces[i->first] = i->second;
    }
}

int cached_polymesh3_loader::find_xmesh_cache_entry_index( const boost::filesystem::path::string_type& filename ) {
    for( std::size_t i = 0; i < m_xmeshCache.size(); ++i ) {
        if( m_xmeshCache[i] && m_xmeshCache[i]->filename == filename ) {
            return static_cast<int>( i );
        }
    }
    return -1;
}

xmesh_cache_entry&
cached_polymesh3_loader::get_xmesh_cache_entry( const boost::filesystem::path::string_type& filename ) {
    int fileIndexOrNegative = find_xmesh_cache_entry_index( filename );

    std::size_t fileIndex;
    if( fileIndexOrNegative >= 0 ) {
        fileIndex = static_cast<std::size_t>( fileIndexOrNegative );
    } else {
        // create a cache entry
        boost::shared_ptr<xmesh_cache_entry> cacheEntry(
            new xmesh_cache_entry( boost::filesystem::path( filename ).native() ) );

        cacheEntry->filename = filename;
        xmesh_metadata metadata;
        read_xmesh_metadata( filename, cacheEntry->metadata );

        // replace the last entry with the new entry
        fileIndex = m_xmeshCache.size() - 1;
        m_xmeshCache[fileIndex] = cacheEntry;
    }

    // move the cache entry to the top of the cache
    for( std::size_t i = fileIndex; i > 0; --i ) {
        m_xmeshCache[i].swap( m_xmeshCache[i - 1] );
    }

    return *( m_xmeshCache[0] );
}

frantic::geometry::const_polymesh3_ptr
cached_polymesh3_loader::build_polymesh3_from_cache( xmesh_reader& reader, polymesh3_cache& cache,
                                                     std::vector<frantic::tstring>& vertexChannelNames,
                                                     std::vector<frantic::tstring>& faceChannelNames, bool loadVerts,
                                                     bool loadFaces, const frantic::tstring& filename ) {
    // build the polymesh
    const xmesh_vertex_channel& geomCh = reader.get_vertex_channel( _T("verts") );

    const std::size_t numVerts = loadVerts ? geomCh.get_vertex_count() : 0;
    const std::size_t numFaceElements = loadFaces ? geomCh.get_face_count() : 0;
    const std::size_t numPolygons = loadFaces ? cache.faces[geomCh.get_face_file_path().native()].numFaces : 0;

    polymesh3_channel_data geomVertData;
    if( numVerts > 0 ) {
        geomVertData = cache.data[geomCh.get_vertex_file_path().native()].data;
    } else {
        frantic::graphics::raw_byte_buffer buffer;
        geomVertData = polymesh3_channel_data( buffer, frantic::channels::data_type_float32, 3 );
    }
    polymesh3_channel_faces geomFacesData =
        ( numFaceElements > 0 ) ? cache.faces[geomCh.get_face_file_path().native()].faces : polymesh3_channel_faces();
    polymesh3_channel_faces geomFacesEndData = ( numFaceElements > 0 )
                                                   ? cache.faces[geomCh.get_face_file_path().native()].facesEnd
                                                   : polymesh3_channel_faces();

    const_shared_polymesh3_builder builder( geomVertData, geomFacesData, geomFacesEndData );

    const_polymesh3_ptr result = builder.finalize();
    polymesh3_const_vertex_accessor<frantic::graphics::vector3f> geomAcc =
        result->get_const_vertex_accessor<frantic::graphics::vector3f>( _T("verts") );

    // check the polymesh channels and add them to the polymesh
    BOOST_FOREACH( const frantic::tstring& vertexChannelName, vertexChannelNames ) {
        if( vertexChannelName == _T("verts") ) {
            continue;
        }
        const xmesh_vertex_channel& ch = reader.get_vertex_channel( vertexChannelName );
        const std::size_t numChannelVerts = ch.get_vertex_count();
        const std::size_t numChannelFaceElements = ch.get_face_count();
        const std::pair<frantic::channels::data_type_t, std::size_t> typeInfo = ch.get_vertex_type();

        assert_expected_cache_data( cache, ch.get_vertex_file_path().native(), typeInfo.first, typeInfo.second,
                                    numChannelVerts );

        if( loadFaces && numChannelFaceElements > 0 ) {
            if( numChannelFaceElements != numFaceElements ) {
                throw xmesh_consistency_error(
                    "load_polymesh3_file() The channel \"" + frantic::strings::to_string( vertexChannelName ) +
                    "\" had mismatched polygon index count compared to the geometry channel in file \"" +
                    frantic::strings::to_string( filename ) + "\"" );
            }

            assert_expected_cache_faces( cache, ch.get_face_file_path().native(), numChannelFaceElements );

            // const std::vector<int> & faceBuffer = cache.faces[ch.get_face_file_path()].faces.get();
            const std::vector<int>& faceEndBuffer = cache.faces[ch.get_face_file_path().native()].facesEnd.get();

            // const int * channelFaceIndexBuffer = &faceBuffer[0];
            for( std::size_t curPoly = 0; curPoly < numPolygons; ++curPoly ) {
                std::size_t curPolySize =
                    ( curPoly == 0 ) ? faceEndBuffer[0] : ( faceEndBuffer[curPoly] - faceEndBuffer[curPoly - 1] );
                if( curPolySize != geomAcc.get_face_degree( curPoly ) )
                    throw xmesh_consistency_error(
                        "load_polymesh_file() The channel \"" + frantic::strings::to_string( vertexChannelName ) +
                        "\" had mismatched polygon sizes compared to the geometry channel in file \"" +
                        frantic::strings::to_string( filename ) + "\"" );
            }

            polymesh3_cache_data_entry& cacheDataEntry = cache.data[ch.get_vertex_file_path().native()];
            builder.add_vertex_channel( vertexChannelName, cacheDataEntry.data,
                                        &cache.faces[ch.get_face_file_path().native()].faces );
        } else {
            polymesh3_cache_data_entry& cacheDataEntry = cache.data[ch.get_vertex_file_path().native()];
            builder.add_vertex_channel( vertexChannelName, cacheDataEntry.data );
        }
    }

    BOOST_FOREACH( const frantic::tstring& faceChannelName, faceChannelNames ) {
        const xmesh_face_channel& ch = reader.get_face_channel( faceChannelName );
        const std::size_t numChannelFaces = ch.get_face_count();
        const std::pair<frantic::channels::data_type_t, std::size_t> typeInfo = ch.get_face_type();

        assert_expected_cache_data( cache, ch.get_face_file_path().native(), typeInfo.first, typeInfo.second,
                                    numChannelFaces );

        if( numChannelFaces != numPolygons ) {
            throw xmesh_consistency_error(
                "load_polymesh_file() The face channel \"" + frantic::strings::to_string( faceChannelName ) +
                "\" did not have one entry per-face in file \"" + frantic::strings::to_string( filename ) + "\"" );
        }

        polymesh3_cache_data_entry& cacheDataEntry = cache.data[ch.get_face_file_path().native()];
        builder.add_face_channel( faceChannelName, cacheDataEntry.data );
    }

    return builder.finalize();
}

frantic::geometry::const_polymesh3_ptr
cached_polymesh3_loader::load_xmesh( const frantic::tstring& filename, xmesh_metadata* outMetadata, int loadMask ) {
    polymesh3_cache& cache = m_cache;

    xmesh_cache_entry& readerCache =
        get_xmesh_cache_entry( boost::filesystem::path( frantic::strings::to_wstring( filename ) ).native() );

    xmesh_reader& reader = readerCache.reader;
    xmesh_metadata& metadata = readerCache.metadata;

    if( loadMask & LOAD_POLYMESH3_MASK::BOX ) {
        if( !metadata.has_boundbox() ) {
            loadMask |= LOAD_POLYMESH3_MASK::VERTS;
        }
    }

    if( loadMask & LOAD_POLYMESH3_MASK::FACES ) {
        loadMask |= LOAD_POLYMESH3_MASK::VERTS;
    }

    if( loadMask & LOAD_POLYMESH3_MASK::VELOCITY ) {
        loadMask |= LOAD_POLYMESH3_MASK::VERTS;
    }

    if( loadMask & LOAD_POLYMESH3_MASK::MAPS ) {
        loadMask |= LOAD_POLYMESH3_MASK::VERTS;
        loadMask |= LOAD_POLYMESH3_MASK::FACES;
    }

    const bool loadVerts = ( loadMask & LOAD_POLYMESH3_MASK::VERTS ) != 0;
    const bool loadFaces = ( loadMask & LOAD_POLYMESH3_MASK::FACES ) != 0;

    // Get vertex channels
    std::vector<frantic::tstring> vertexChannelNames;
    // all of the channels in vertexChannelNames, plus sometimes Velocity
    // because I don't want it repeatedly loaded and unloaded while scrubbing
    std::vector<frantic::tstring> keepVertexChannelNames;
    {
        std::vector<frantic::tstring> allVertexChannelNames;
        reader.get_vertex_channel_names( allVertexChannelNames );
        vertexChannelNames.reserve( allVertexChannelNames.size() + 1 );
        keepVertexChannelNames.reserve( allVertexChannelNames.size() + 1 );
        BOOST_FOREACH( const frantic::tstring& channelName, allVertexChannelNames ) {
            if( channelName == _T("Velocity") ) {
                if( loadMask & LOAD_POLYMESH3_MASK::VELOCITY ) {
                    vertexChannelNames.push_back( channelName );
                } else if( loadVerts ) {
                    keepVertexChannelNames.push_back( channelName );
                }
            } else if( loadMask & LOAD_POLYMESH3_MASK::MAPS ) {
                vertexChannelNames.push_back( channelName );
            }
        }
    }
    if( loadVerts ) {
        vertexChannelNames.push_back( _T("verts") );
    }
    keepVertexChannelNames.insert( keepVertexChannelNames.end(), vertexChannelNames.begin(), vertexChannelNames.end() );

    // Get face channels
    std::vector<frantic::tstring> faceChannelNames;
    if( loadMask & LOAD_POLYMESH3_MASK::FACES ) {
        reader.get_face_channel_names( faceChannelNames );
    }
    std::vector<frantic::tstring> keepFaceChannelNames( faceChannelNames );

    // Grab a reference to all channels that we need for the current mesh
    // before we evict cache entries
    { // scope for channel references
        std::vector<polymesh3_channel_data> keepData;
        std::vector<polymesh3_channel_faces> keepFaces;
        get_keep_channel_refs( reader, cache, keepVertexChannelNames, loadFaces, keepFaceChannelNames, keepData,
                               keepFaces );

        // Evict unreferenced cache entries
        // TODO: Keep unreferenced cache entries with some memory limit and eviction policy
        // TODO: Reuse memory allocated for evicted entries?
        std::vector<boost::filesystem::path::string_type> evictData;
        for( polymesh3_cache::data_cache_t::iterator i = cache.data.begin(); i != cache.data.end(); ++i ) {
            if( !i->second.data.is_valid() ) {
                evictData.push_back( i->first );
            } else if( !i->second.data.is_shared() ) {
                evictData.push_back( i->first );
            }
        }
        BOOST_FOREACH( const boost::filesystem::path::string_type& channelFileName, evictData ) {
            cache.data.erase( channelFileName );
        }
        std::vector<boost::filesystem::path::string_type> evictFaces;
        for( polymesh3_cache::faces_cache_t::iterator i = cache.faces.begin(); i != cache.faces.end(); ++i ) {
            if( !i->second.faces.is_valid() || !i->second.facesEnd.is_valid() ) {
                evictFaces.push_back( i->first );
            } else if( !i->second.faces.is_shared() ) {
                evictFaces.push_back( i->first );
            }
        }
        BOOST_FOREACH( const boost::filesystem::path::string_type& channelFileName, evictFaces ) {
            cache.faces.erase( channelFileName );
        }
    }

    cache_missing_files( filename, reader, cache, vertexChannelNames, loadFaces, faceChannelNames, m_threadCount );

    frantic::geometry::const_polymesh3_ptr result = build_polymesh3_from_cache(
        reader, cache, vertexChannelNames, faceChannelNames, loadVerts, loadFaces, filename );

    if( loadMask & LOAD_POLYMESH3_MASK::BOX ) {
        if( !metadata.has_boundbox() ) {
            frantic::graphics::boundbox3f bbox;
            if( result->has_vertex_channel( _T("verts") ) ) {
                frantic::geometry::polymesh3_const_vertex_accessor<frantic::graphics::vector3f> acc =
                    result->get_const_vertex_accessor<frantic::graphics::vector3f>( _T("verts") );
                for( std::size_t i = 0; i < acc.vertex_count(); ++i ) {
                    bbox += acc.get_vertex( i );
                }
            }
            metadata.set_boundbox( bbox );
        }
    }

    if( outMetadata ) {
        *outMetadata = metadata;
    }

    return result;
}

frantic::geometry::const_polymesh3_ptr cached_polymesh3_loader::load( const frantic::tstring& filename,
                                                                      frantic::geometry::xmesh_metadata* outMetadata,
                                                                      int loadMask ) {
    using namespace frantic::channels;

    FF_LOG( debug ) << "Loading file: \"" << filename << "\"" << std::endl;

    boost::timer timer;

    frantic::geometry::const_polymesh3_ptr result;

    if( outMetadata ) {
        outMetadata->clear();
    }

    const frantic::tstring type = frantic::strings::to_lower( frantic::files::extension_from_path( filename ) );
    if( type == _T(".obj") ) {
        result = load_obj_polymesh_file( filename );
    } else if( type == _T(".xmesh") ) {
        result = load_xmesh( filename, outMetadata, loadMask );
    } else {
        throw std::runtime_error(
            "cached_polymesh3_loader::load() Didn't recognize the file format of the input mesh file \"" +
            frantic::strings::to_string( filename ) + "\"" );
    }

    if( !result ) {
        throw std::runtime_error( "cached_polymesh3_loader::load() Result is NULL" );
    }

    if( ( loadMask & LOAD_POLYMESH3_MASK::BOX ) && outMetadata && !outMetadata->has_boundbox() ) {
        frantic::graphics::boundbox3f bbox;
        if( result->has_vertex_channel( _T("verts") ) ) {
            frantic::geometry::polymesh3_const_vertex_accessor<frantic::graphics::vector3f> acc =
                result->get_const_vertex_accessor<frantic::graphics::vector3f>( _T("verts") );
            for( std::size_t i = 0; i < acc.vertex_count(); ++i ) {
                bbox += acc.get_vertex( i );
            }
        }
        outMetadata->set_boundbox( bbox );
    }

    FF_LOG( debug ) << "Load time [s]: " << boost::basic_format<frantic::tchar>( _T("%.3f") ) % timer.elapsed()
                    << std::endl;

    return result;
}

void cached_polymesh3_loader::set_thread_count( std::size_t numThreads ) {
    if( numThreads >= 1 ) {
        m_threadCount = numThreads;
    } else {
        m_threadCount = 1;
    }
}

void cached_polymesh3_loader::clear_cache() {
    m_cache.data.clear();
    m_cache.faces.clear();
    for( std::size_t i = 0; i < m_xmeshCache.size(); ++i ) {
        m_xmeshCache[i].reset();
    }
}

#else

namespace {

int fill_load_mask_dependencies( int loadMask ) {
    if( loadMask & LOAD_POLYMESH3_MASK::FACES ) {
        loadMask |= LOAD_POLYMESH3_MASK::VERTS;
    }

    if( loadMask & LOAD_POLYMESH3_MASK::VELOCITY ) {
        loadMask |= LOAD_POLYMESH3_MASK::VERTS;
    }

    if( loadMask & LOAD_POLYMESH3_MASK::MAPS ) {
        loadMask |= LOAD_POLYMESH3_MASK::VERTS;
        loadMask |= LOAD_POLYMESH3_MASK::FACES;
    }

    return loadMask;
}

bool get_load_faces( int loadMask ) {
    return ( fill_load_mask_dependencies( loadMask ) & LOAD_POLYMESH3_MASK::FACES ) != 0;
}

frantic::channels::channel_propagation_policy get_channel_propagation_policy( int loadMask ) {
    loadMask = fill_load_mask_dependencies( loadMask );

    frantic::channels::channel_propagation_policy cpp;

    if( loadMask & LOAD_POLYMESH3_MASK::MAPS ) {
        cpp.set_to_exclude_policy();
        if( ( loadMask & LOAD_POLYMESH3_MASK::VELOCITY ) == 0 ) {
            cpp.add_channel( _T("Velocity") );
        }
    } else {
        cpp.set_to_include_policy();
        if( loadMask & LOAD_POLYMESH3_MASK::VELOCITY ) {
            cpp.add_channel( _T("Velocity") );
        }
    }

    return cpp;
}

frantic::geometry::polymesh3_ptr load_polymesh3( const frantic::tstring& filename, xmesh_metadata* outMetadata,
                                                 int loadMask ) {
    frantic::geometry::polymesh3_ptr result;

    const frantic::tstring type = frantic::strings::to_lower( frantic::files::extension_from_path( filename ) );
    if( type == _T(".obj") ) {
        result = load_obj_polymesh_file( filename );
        if( outMetadata ) {
            outMetadata->clear();
        }
    } else if( type == _T(".xmesh") ) {
        bool done = false;
        frantic::geometry::xmesh_reader reader( filename );
        if( outMetadata ) {
            *outMetadata = reader.get_metadata();
        }
        if( loadMask == LOAD_POLYMESH3_MASK::BOX && outMetadata && outMetadata->has_boundbox() ) {
            frantic::geometry::polymesh3_builder builder;
            result = builder.finalize();
            done = true;
        }
        if( !done ) {
            result = frantic::geometry::load_xmesh_polymesh_file( reader, get_channel_propagation_policy( loadMask ),
                                                                  get_load_faces( loadMask ) );
        }
    } else {
        throw std::runtime_error( "load_polymesh3: Didn't recognize the file format of the input mesh file \"" +
                                  frantic::strings::to_string( filename ) + "\"" );
    }

    if( outMetadata && !outMetadata->has_boundbox() ) {
        frantic::graphics::boundbox3f bbox;
        if( result->has_vertex_channel( _T("verts") ) ) {
            frantic::geometry::polymesh3_const_vertex_accessor<frantic::graphics::vector3f> acc =
                result->get_const_vertex_accessor<frantic::graphics::vector3f>( _T("verts") );
            for( std::size_t i = 0; i < acc.vertex_count(); ++i ) {
                bbox += acc.get_vertex( i );
            }
        }
        outMetadata->set_boundbox( bbox );
    }

    return result;
}

} // anonymous namespace

cached_polymesh3_loader::cached_polymesh3_loader()
    : m_cachedMask( 0 ) {}

frantic::geometry::const_polymesh3_ptr cached_polymesh3_loader::load( const frantic::tstring& filename,
                                                                      xmesh_metadata* outMetadata, int loadMask ) {
    frantic::geometry::polymesh3_ptr result;

    boost::unique_lock<mutex_t> lock( m_cachedMeshMutex );

    const int missingMask = ~m_cachedMask & loadMask;

    if( filename == m_cachedMeshFilename && missingMask == 0 ) {
        result = m_cachedMesh;
    } else {
        lock.unlock();
        xmesh_metadata metadata;
        result = load_polymesh3( filename, &metadata, loadMask );
        lock.lock();
        m_cachedMesh = result;
        m_cachedMeshMetadata = metadata;
        m_cachedMeshFilename = filename;
        m_cachedMask = loadMask;
    }

    if( outMetadata ) {
        *outMetadata = m_cachedMeshMetadata;
    }

    return result;
}

void cached_polymesh3_loader::set_thread_count( std::size_t /*numThreads*/ ) {}

void cached_polymesh3_loader::clear_cache() {
    boost::unique_lock<mutex_t> lock( m_cachedMeshMutex );
    m_cachedMeshFilename.clear();
    m_cachedMesh.reset();
}

#endif
