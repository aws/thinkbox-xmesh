# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
cmake_minimum_required( VERSION 3.15 FATAL_ERROR )

project( XMeshCore )

find_package( thinkboxcmlibrary REQUIRED )
include( ThinkboxCMLibrary )
include( PrecompiledHeader )

option(BUILD_UNIT_TESTS "Build unit tests" ON)
option(BUILD_WITH_TBB "Build with Intel TBB" ON)

add_library( xmeshcore STATIC )

target_include_directories( xmeshcore PUBLIC
	$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
	$<INSTALL_INTERFACE:include>
)

file( GLOB_RECURSE H_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    "xmesh/*.h"
    "xmesh/*.hpp"
)

file( GLOB_RECURSE CXX_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    "src/xmesh/*.cpp"
)

target_sources( xmeshcore PRIVATE
    stdafx.cpp
    stdafx.h
    ${H_FILES}
    ${CXX_FILES}
)

# The Conan version of Boost was built with this, and it changes the library names.
# As a result, we need to set this to tell Boost to look for the right libraries to
# link against.
target_compile_definitions( xmeshcore PUBLIC BOOST_AUTO_LINK_SYSTEM )

set_target_properties( xmeshcore PROPERTIES PROJECT_LABEL "XMeshCore" )

find_package( thinkboxlibrary REQUIRED )
find_package( Boost REQUIRED )
find_package( OpenEXR REQUIRED )
find_package( tinyxml2 REQUIRED )
find_package( ZLIB REQUIRED )

target_include_directories( xmeshcore PUBLIC ${thinkboxlibrary_INCLUDE_DIRS} )
target_include_directories( xmeshcore PUBLIC ${Boost_INCLUDE_DIRS} )
target_include_directories( xmeshcore PUBLIC ${OpenEXR_INCLUDE_DIRS} )
target_include_directories( xmeshcore PUBLIC ${tinyxml2_INCLUDE_DIRS} )
target_include_directories( xmeshcore PUBLIC ${ZLIB_INCLUDE_DIRS} )

target_link_libraries( xmeshcore INTERFACE thinkboxlibrary::thinkboxlibrary )
target_link_libraries( xmeshcore INTERFACE Boost::Boost )
target_link_libraries( xmeshcore INTERFACE OpenEXR::OpenEXR )
target_link_libraries( xmeshcore INTERFACE tinyxml2::tinyxml2 )
target_link_libraries( xmeshcore INTERFACE ZLIB::ZLIB )

if(BUILD_WITH_TBB)
    target_compile_definitions( xmeshcore PRIVATE TBB_AVAILABLE )
    find_package( TBB REQUIRED )
    target_include_directories( xmeshcore PUBLIC ${TBB_INCLUDE_DIRS} )
    target_link_libraries( xmeshcore INTERFACE TBB::tbb )
endif()

frantic_default_source_groups( xmeshcore HEADERDIR xmesh SOURCEDIR src/xmesh )
frantic_common_platform_setup( xmeshcore )

add_precompiled_header( xmeshcore stdafx.h SOURCE_CXX stdafx.cpp )

# Disable optimization for the RelWithDebInfo configuration on Windows.
# This allows breakpoints to be hit reliably when debugging in Visual Studio.
if( WIN32 )
	target_compile_options( xmeshcore PRIVATE "$<$<CONFIG:Release>:/O2>$<$<CONFIG:RelWithDebInfo>:/Od>" )
endif()

if( BUILD_UNIT_TESTS )
    add_subdirectory( UnitTests )
endif()

install( DIRECTORY xmesh
		 DESTINATION include
		 FILES_MATCHING PATTERN "*.hpp"
)
install( TARGETS xmeshcore
		 RUNTIME DESTINATION bin
		 LIBRARY DESTINATION lib
		 ARCHIVE DESTINATION lib
)
