// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#include "stdafx.h"

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <boost/foreach.hpp>

#include <frantic/files/paths.hpp>
#include <frantic/files/scoped_file_cleanup.hpp>

#include <frantic/geometry/polymesh3.hpp>
#include <frantic/geometry/polymesh3_builder.hpp>
#include <frantic/geometry/polymesh3_file_io.hpp>
#include <frantic/geometry/xmesh_sequence_saver.hpp>

#include <xmesh/cached_polymesh3_loader.hpp>

using namespace boost::filesystem;

using namespace frantic::channels;
using namespace frantic::geometry;
using namespace frantic::graphics;

using namespace xmesh;

namespace {

polymesh3_ptr create_test_mesh() {
    polymesh3_builder builder;

    builder.add_vertex( 0, 0, 0 );
    builder.add_vertex( 1, 0, 0 );
    builder.add_vertex( 1, 1, 0 );

    int indices[] = { 0, 1, 2 };

    builder.add_polygon( indices, 3 );

    polymesh3_ptr mesh = builder.finalize();

    raw_byte_buffer buffer;
    buffer.resize( 3 * sizeof( vector3f ) );
    vector3f* v = reinterpret_cast<vector3f*>( buffer.begin() );
    v[0] = vector3f( 0, 0, 1 );
    v[1] = vector3f( 0, 0, 1 );
    v[2] = vector3f( 0, 0, 1 );

    mesh->add_vertex_channel( _T("Velocity"), data_type_float32, 3, buffer );

    buffer.resize( 3 * sizeof( vector3f ) );
    v = reinterpret_cast<vector3f*>( buffer.begin() );
    v[0] = vector3f( 1, 0, 0 );
    v[1] = vector3f( 0, 1, 0 );
    v[2] = vector3f( 0, 0, 1 );
    mesh->add_vertex_channel( _T("Color"), data_type_float32, 3, buffer );

    buffer.resize( 3 * sizeof( vector3f ) );
    v = reinterpret_cast<vector3f*>( buffer.begin() );
    v[0] = vector3f( 0, 0, 1 );
    v[1] = vector3f( 0, 0, 1 );
    v[2] = vector3f( 0, 0, 1 );
    mesh->add_vertex_channel( _T("Normal"), data_type_float32, 3, buffer );

    buffer.resize( 2 * sizeof( vector3f ) );
    v = reinterpret_cast<vector3f*>( buffer.begin() );
    v[0] = vector3f( 1, 0, 0 );
    v[1] = vector3f( 2, 0, 0 );
    std::vector<int> faces( 3 );
    faces[0] = 0;
    faces[1] = 1;
    faces[2] = 0;
    mesh->add_vertex_channel( _T("TextureCoord"), data_type_float32, 3, buffer, &faces );

    buffer.resize( sizeof( boost::uint16_t ) );
    boost::uint16_t* i = reinterpret_cast<boost::uint16_t*>( buffer.begin() );
    i[0] = 4;
    mesh->add_face_channel( _T("MaterialID"), data_type_uint16, 1, buffer );

    buffer.resize( sizeof( boost::int32_t ) );
    boost::int32_t* i32 = reinterpret_cast<boost::int32_t*>( buffer.begin() );
    i32[0] = 1;
    mesh->add_face_channel( _T("SmoothingGroup"), data_type_int32, 1, buffer );

    return mesh;
}

} // anonymous namespace

TEST( CachedPolymesh3Loader, LoadObj ) {
    path tempDir = temp_directory_path() / unique_path();
    create_directory( tempDir );

    frantic::files::scoped_file_cleanup cleanup;
    cleanup.add( tempDir );

    const frantic::tstring filename = frantic::files::to_tstring( tempDir / "temp.obj" );

    polymesh3_ptr mesh = create_test_mesh();

    // Filter to keep only channels that are supported by OBJ
    channel_propagation_policy cpp( true );
    cpp.add_channel( _T("Normal") );
    cpp.add_channel( _T("TextureCoord") );

    {
        std::vector<frantic::tstring> channels;
        mesh->get_vertex_channel_names( channels );
        BOOST_FOREACH( const frantic::tstring& channel, channels ) {
            if( !cpp.is_channel_included( channel ) ) {
                mesh->erase_vertex_channel( channel );
            }
        }
    }

    {
        std::vector<frantic::tstring> channels;
        mesh->get_face_channel_names( channels );
        BOOST_FOREACH( const frantic::tstring& channel, channels ) {
            if( !cpp.is_channel_included( channel ) ) {
                mesh->erase_face_channel( channel );
            }
        }
    }

    EXPECT_TRUE( mesh->has_vertex_channel( _T("TextureCoord") ) );

    write_obj_polymesh_file( filename, mesh );

    xmesh_metadata metadata;

    cached_polymesh3_loader loader;
    const_polymesh3_ptr loadedMesh = loader.load( filename, &metadata, LOAD_POLYMESH3_MASK::ALL );

    EXPECT_TRUE( is_equal( mesh, loadedMesh ) );
}

TEST( CachedPolymesh3Loader, LoadEmptyXMesh ) {
    path tempDir = temp_directory_path() / unique_path();
    create_directory( tempDir );

    frantic::files::scoped_file_cleanup cleanup;
    cleanup.add( tempDir );

    const frantic::tstring filename = frantic::files::to_tstring( tempDir / "temp.xmesh" );

    polymesh3_builder builder;

    polymesh3_ptr mesh = builder.finalize();

    raw_byte_buffer buffer;
    std::vector<int> faces;

    mesh->add_vertex_channel( _T("Velocity"), data_type_float32, 3, buffer );
    mesh->add_vertex_channel( _T("TextureCoord"), data_type_float32, 3, buffer, &faces );
    mesh->add_face_channel( _T("MaterialID"), data_type_uint16, 1, buffer );

    write_xmesh_polymesh_file( filename, mesh, 0 );

    xmesh_metadata metadata;

    cached_polymesh3_loader loader;
    const_polymesh3_ptr loadedMesh = loader.load( filename, &metadata, LOAD_POLYMESH3_MASK::ALL );

    EXPECT_TRUE( is_equal( mesh, loadedMesh ) );
}

TEST( CachedPolymesh3Loader, LoadXMesh ) {
    path tempDir = temp_directory_path() / unique_path();
    create_directory( tempDir );

    frantic::files::scoped_file_cleanup cleanup;
    cleanup.add( tempDir );

    polymesh3_ptr meshA = create_test_mesh();
    polymesh3_ptr meshB = create_test_mesh();
    {
        // change some of meshB's channel values

        polymesh3_vertex_accessor<vector3f> verts = meshB->get_vertex_accessor<vector3f>( _T("verts") );
        for( std::size_t i = 0; i < verts.vertex_count(); ++i ) {
            verts.get_vertex( i ).z = 1;
        }

        polymesh3_vertex_accessor<vector3f> color = meshB->get_vertex_accessor<vector3f>( _T("Color") );
        for( std::size_t i = 0; i < color.vertex_count(); ++i ) {
            color.get_vertex( i ).set( 0, 1, 0 );
        }

        raw_byte_buffer buffer;
        buffer.resize( 2 * sizeof( vector3f ) );
        vector3f* v = reinterpret_cast<vector3f*>( buffer.begin() );
        v[0] = vector3f( 1, 0, 0 );
        v[1] = vector3f( 2, 0, 0 );
        std::vector<int> faces( 3 );
        faces[0] = 1;
        faces[1] = 0;
        faces[2] = 1;
        meshB->erase_vertex_channel( _T("TextureCoord") );
        meshB->add_vertex_channel( _T("TextureCoord"), data_type_float32, 3, buffer, &faces );

        polymesh3_face_accessor<boost::uint16_t> materialID =
            meshB->get_face_accessor<boost::uint16_t>( _T("MaterialID") );
        materialID.get_face( 0 ) = 8;
    }

    const frantic::tstring file0 = frantic::files::to_tstring( tempDir / "moving_triangle_0000.xmesh" );
    const frantic::tstring file1 = frantic::files::to_tstring( tempDir / "moving_triangle_0001.xmesh" );

    xmesh_sequence_saver xss;
    xss.write_xmesh( meshA, file0 );
    xss.write_xmesh( meshB, file1 );

    xmesh_metadata metadata;

    cached_polymesh3_loader loader;
    const_polymesh3_ptr mesh0 = loader.load( file0, &metadata, LOAD_POLYMESH3_MASK::ALL );
    polymesh3_ptr mesh( const_cast<polymesh3*>( mesh0.get() ) );
    EXPECT_TRUE( is_equal( meshA, mesh ) );

    mesh0 = loader.load( file1, &metadata, LOAD_POLYMESH3_MASK::ALL );
    mesh.reset( const_cast<polymesh3*>( mesh0.get() ) );
    EXPECT_TRUE( is_equal( meshB, mesh ) );

    {
        cached_polymesh3_loader loader;
        const_polymesh3_ptr mesh = loader.load( file0, &metadata, LOAD_POLYMESH3_MASK::BOX );
        EXPECT_EQ( 0, mesh->vertex_count() );
        EXPECT_EQ( 0, mesh->face_count() );
    }

    {
        cached_polymesh3_loader loader;
        const_polymesh3_ptr mesh = loader.load( file0, &metadata, LOAD_POLYMESH3_MASK::VERTS );
        EXPECT_EQ( 3, mesh->vertex_count() );
        EXPECT_EQ( 0, mesh->face_count() );
        EXPECT_EQ( vector3f( 0, 0, 0 ), mesh->get_vertex( 0 ) );
        EXPECT_EQ( vector3f( 1, 0, 0 ), mesh->get_vertex( 1 ) );
        EXPECT_EQ( vector3f( 1, 1, 0 ), mesh->get_vertex( 2 ) );
        EXPECT_FALSE( mesh->has_vertex_channel( _T("Velocity") ) );
        EXPECT_FALSE( mesh->has_vertex_channel( _T("Color") ) );
    }

    {
        cached_polymesh3_loader loader;
        const_polymesh3_ptr mesh = loader.load( file0, &metadata, LOAD_POLYMESH3_MASK::FACES );
        EXPECT_EQ( 3, mesh->vertex_count() );
        EXPECT_EQ( 1, mesh->face_count() );
        EXPECT_EQ( vector3f( 0, 0, 0 ), mesh->get_vertex( 0 ) );
        EXPECT_EQ( vector3f( 1, 0, 0 ), mesh->get_vertex( 1 ) );
        EXPECT_EQ( vector3f( 1, 1, 0 ), mesh->get_vertex( 2 ) );
    }

    {
        cached_polymesh3_loader loader;
        const_polymesh3_ptr mesh = loader.load( file0, &metadata, LOAD_POLYMESH3_MASK::VELOCITY );
        EXPECT_EQ( 3, mesh->vertex_count() );
        EXPECT_EQ( 0, mesh->face_count() );
        EXPECT_EQ( vector3f( 0, 0, 0 ), mesh->get_vertex( 0 ) );
        EXPECT_EQ( vector3f( 1, 0, 0 ), mesh->get_vertex( 1 ) );
        EXPECT_EQ( vector3f( 1, 1, 0 ), mesh->get_vertex( 2 ) );
        EXPECT_TRUE( mesh->has_vertex_channel( _T("Velocity") ) );
    }

    {
        cached_polymesh3_loader loader;
        const_polymesh3_ptr mesh = loader.load( file0, &metadata, LOAD_POLYMESH3_MASK::MAPS );
        EXPECT_EQ( 3, mesh->vertex_count() );
        EXPECT_EQ( 1, mesh->face_count() );
        EXPECT_EQ( vector3f( 0, 0, 0 ), mesh->get_vertex( 0 ) );
        EXPECT_EQ( vector3f( 1, 0, 0 ), mesh->get_vertex( 1 ) );
        EXPECT_EQ( vector3f( 1, 1, 0 ), mesh->get_vertex( 2 ) );
        EXPECT_FALSE( mesh->has_vertex_channel( _T("Velocity") ) );
        EXPECT_TRUE( mesh->has_vertex_channel( _T("Color") ) );
        EXPECT_TRUE( mesh->has_vertex_channel( _T("Normal") ) );
        EXPECT_TRUE( mesh->has_vertex_channel( _T("TextureCoord") ) );
        EXPECT_TRUE( mesh->has_face_channel( _T("MaterialID") ) );
        EXPECT_TRUE( mesh->has_face_channel( _T("SmoothingGroup") ) );
    }

    {
        cached_polymesh3_loader loader;
        const_polymesh3_ptr mesh = loader.load( file0, &metadata, LOAD_POLYMESH3_MASK::STATIC_MESH );
        EXPECT_EQ( 3, mesh->vertex_count() );
        EXPECT_EQ( 1, mesh->face_count() );
        EXPECT_EQ( vector3f( 0, 0, 0 ), mesh->get_vertex( 0 ) );
        EXPECT_EQ( vector3f( 1, 0, 0 ), mesh->get_vertex( 1 ) );
        EXPECT_EQ( vector3f( 1, 1, 0 ), mesh->get_vertex( 2 ) );
        EXPECT_FALSE( mesh->has_vertex_channel( _T("Velocity") ) );
        EXPECT_TRUE( mesh->has_vertex_channel( _T("Color") ) );
        EXPECT_TRUE( mesh->has_vertex_channel( _T("Normal") ) );
        EXPECT_TRUE( mesh->has_vertex_channel( _T("TextureCoord") ) );
        EXPECT_TRUE( mesh->has_face_channel( _T("MaterialID") ) );
        EXPECT_TRUE( mesh->has_face_channel( _T("SmoothingGroup") ) );
    }
}

TEST( CachedPolymesh3Loader, PopulateXMeshBoundBoxMetadata ) {
    path tempDir = temp_directory_path() / unique_path();
    create_directory( tempDir );

    frantic::files::scoped_file_cleanup cleanup;
    cleanup.add( tempDir );

    const frantic::tstring file = frantic::files::to_tstring( tempDir / "box.xmesh" );

    {
        polymesh3_builder builder;

        builder.add_vertex( 0, 0, 0 );
        builder.add_vertex( 1, 0, 0 );
        builder.add_vertex( 1, 1, 1 );

        int indices[] = { 0, 1, 2 };

        builder.add_polygon( indices, 3 );

        polymesh3_ptr mesh = builder.finalize();

        write_xmesh_polymesh_file( file, mesh );
    }

    // Currently, write_xmesh_polymesh_file() doesn't write boundbox metadata.
    // It isn't a problem if it does, but I want some way to test that
    // cached_polymesh3_loader::load() populates the boundbox metadata even
    // when the loaded file doesn't have such metadata.
    {
        xmesh_metadata metadata;
        read_xmesh_metadata( file, metadata );
        EXPECT_FALSE( metadata.has_boundbox() );
    }

    xmesh_metadata metadata;
    cached_polymesh3_loader loader;
    loader.load( file, &metadata, LOAD_POLYMESH3_MASK::BOX );
    EXPECT_TRUE( metadata.has_boundbox() );
    EXPECT_EQ( boundbox3f( vector3f( 0 ), vector3f( 1 ) ), metadata.get_boundbox() );
}

TEST( CachedPolymesh3Loader, PopulateObjBoundBoxMetadata ) {
    path tempDir = temp_directory_path() / unique_path();
    create_directory( tempDir );

    frantic::files::scoped_file_cleanup cleanup;
    cleanup.add( tempDir );

    const frantic::tstring file = frantic::files::to_tstring( tempDir / "box.obj" );

    {
        polymesh3_builder builder;

        builder.add_vertex( 0, 0, 0 );
        builder.add_vertex( 1, 0, 0 );
        builder.add_vertex( 1, 1, 1 );

        int indices[] = { 0, 1, 2 };

        builder.add_polygon( indices, 3 );

        polymesh3_ptr mesh = builder.finalize();

        write_obj_polymesh_file( file, mesh );
    }

    xmesh_metadata metadata;
    cached_polymesh3_loader loader;
    loader.load( file, &metadata, LOAD_POLYMESH3_MASK::BOX );
    EXPECT_TRUE( metadata.has_boundbox() );
    EXPECT_EQ( boundbox3f( vector3f( 0 ), vector3f( 1 ) ), metadata.get_boundbox() );
}
