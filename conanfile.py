# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os 
from conans import ConanFile, CMake

SETTINGS: list[str] = [
    'os',
    'compiler',
    'build_type',
    'arch'
]

TOOL_REQUIRES: list[str]= [
    'cmake/3.24.1',
    'thinkboxcmlibrary/1.0.0'
]

REQUIRES: list[str] = [
    'thinkboxlibrary/1.0.0',
    'tinyxml2/9.0.0'
]

class XMeshCoreConan(ConanFile):
    name: str = 'xmeshcore'
    version: str = '1.0.0'
    license: str = 'Apache-2.0'
    description: str = 'Shared XMesh code.'
    settings: list[str] = SETTINGS
    requires: list[str] = REQUIRES
    tool_requires: list[str] = TOOL_REQUIRES
    generators: str | list[str] = 'cmake_find_package'

    def build(self) -> None:
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def imports(self) -> None:
        # Copy tbb DLLs to the UnitTests binary directory
        self.copy('*.dll', dst='UnitTests/Release', src='bin')

    def export_sources(self) -> None:
        self.copy('**.h', src='', dst='')
        self.copy('**.hpp', src='', dst='')
        self.copy('**.cpp', src='', dst='')
        self.copy('**.cc', src='', dst='')
        self.copy('CMakeLists.txt', src='', dst='')
        self.copy('UnitTests/CMakeLists.txt', src='', dst='')
        self.copy('NOTICE.txt', src='', dst='')
        self.copy('LICENSE.txt', src='', dst='')
        self.copy('THIRD-PARTY-LICENSES', src='', dst='')

    def package(self) -> None:
        cmake = CMake(self)
        cmake.install()

        with open(os.path.join(self.source_folder, 'NOTICE.txt'), 'r', encoding='utf8') as notice_file:
            notice_contents = notice_file.readlines()
        with open(os.path.join(self.source_folder, 'LICENSE.txt'), 'r', encoding='utf8') as license_file:
            license_contents = license_file.readlines()
        with open(os.path.join(self.source_folder, 'THIRD-PARTY-LICENSES'), 'r', encoding='utf8') as third_party_file:
            third_party_contents = third_party_file.readlines()
        os.makedirs(os.path.join(self.package_folder, 'licenses'), exist_ok=True)
        with open(os.path.join(self.package_folder, 'licenses', 'LICENSE'), 'w', encoding='utf8') as cat_license_file:
            cat_license_file.writelines(notice_contents)
            cat_license_file.writelines(license_contents)
            cat_license_file.writelines(third_party_contents)

    def deploy(self) -> None:
        self.copy("*", dst="lib", src="lib")
        self.copy("*", dst="include", src="include")

    def package_info(self) -> None:
        self.cpp_info.libs = ["xmeshcore"]
