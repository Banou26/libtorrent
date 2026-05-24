# libtorrent → WASM (single-threaded, browser, @fkn/lib transport)
#
# This file is invoked via emcmake from the parent build:
#
#   nix develop ./bindings/wasm   # provides emsdk + boost source + cmake
#   cd bindings/wasm
#   emcmake cmake -B build -S . \
#       -DBOOST_ROOT=$BOOST_ROOT \
#       -DCMAKE_BUILD_TYPE=Release
#   cmake --build build -j
#
# The output is build/libtorrent.{js,wasm}, ready to be imported from the
# host page or web worker (see js/index.ts).

cmake_minimum_required(VERSION 3.20)
project(libtorrent_wasm CXX)

if (NOT EMSCRIPTEN)
  message(FATAL_ERROR "Configure this with emcmake; it is Emscripten-only.")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ----- libtorrent: pull the in-tree sources, no system install needed -------
set(LT_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")
file(GLOB LT_SOURCES
  "${LT_ROOT}/src/*.cpp"
  "${LT_ROOT}/src/kademlia/*.cpp"
  "${LT_ROOT}/src/ed25519/*.cpp"
)

# Drop only sources we *replace* with the WASM-specific implementations,
# plus a few that rely on facilities we won't run (UPnP/NAT-PMP — the
# browser can't talk LAN multicast). Keep enum_net, file*, posix_* — they
# expose helpers session_impl references at link time even when the
# code paths are turned off via TORRENT_USE_*.
list(FILTER LT_SOURCES EXCLUDE REGEX "/(mmap_storage|mmap_disk_io|file_view_pool|file_mapping|file_pointer|drive_info)\\.cpp$")

# ----- libtorrent compile defines ------------------------------------------
# Emscripten's wasm32-unknown-emscripten target doesn't set __linux__, so
# libtorrent's config.hpp falls into the "unknown OS, assuming BSD" path.
# Force the Linux branch — it's the closest match for Emscripten's musl-based
# libc.
#
# Note: settings tested with `#if FOO` take 0/1 values; settings tested with
# `#ifdef FOO` must be UNDEFINED (passing FOO=0 still triggers `#ifdef`).
# That's why TORRENT_USE_OPENSSL/GNUTLS don't appear below — leaving them
# undefined is exactly what we want.
set(LT_DEFS
  TORRENT_BUILDING_LIBRARY
  TORRENT_USE_I2P=0
  BOOST_ASIO_DISABLE_EPOLL
  BOOST_ASIO_DISABLE_KQUEUE
  BOOST_ASIO_DISABLE_EVENTFD
  BOOST_ASIO_DISABLE_THREADS
  BOOST_ASIO_HAS_STD_CHRONO
  BOOST_ASIO_NO_DEPRECATED
  BOOST_DATE_TIME_NO_LIB
  BOOST_REGEX_NO_LIB
  BOOST_EXCEPTION_DISABLE
  BOOST_SYSTEM_NO_DEPRECATED
  BOOST_ALL_NO_LIB
)

# ----- target ---------------------------------------------------------------
add_executable(libtorrent
  ${LT_SOURCES}
  src/wrapper.cpp
  src/disk_io.cpp
)

if (NOT BOOST_ROOT)
  set(BOOST_ROOT "$ENV{BOOST_ROOT}")
endif()
if (NOT BOOST_ROOT)
  message(FATAL_ERROR "BOOST_ROOT not set — pass -DBOOST_ROOT=… or enter the nix shell")
endif()

target_include_directories(libtorrent PRIVATE
  "${LT_ROOT}/include"
  "${LT_ROOT}/deps/try_signal"
  "${BOOST_ROOT}"  # boost is header-only for what we need (asio + system)
)

target_compile_definitions(libtorrent PRIVATE ${LT_DEFS})

target_compile_options(libtorrent PRIVATE
  -O3 -fexceptions
  -Wno-deprecated-declarations
)

# Diagnostic option — set CMake var DEBUG_NAMES=1 to keep import names
# unmangled so `WebAssembly.Module.imports()` is readable.
option(DEBUG_NAMES "Keep WASM imports unmangled (diagnostic)" ON)

# ----- Emscripten link flags ------------------------------------------------
set(EM_LINK_FLAGS
  # Standalone module, importable as ES module from a worker.
  "-sMODULARIZE=1"
  "-sEXPORT_ES6=1"
  "-sENVIRONMENT=worker,web"

  # We never SAB; ensure no pthread enablement leaks in.
  "-sUSE_PTHREADS=0"
  "-sWASM=1"
  # uint64_t job_id parameters are passed as (lo, hi) i32 pairs from JS.
  # Default WASM_BIGINT=1 in newer Emscripten would pass them as a single
  # bigint and break those calls. Force the split-i32 ABI.
  "-sWASM_BIGINT=0"

  # The host wires this up before the module instantiates.
  "-sINCOMING_MODULE_JS_API=['print','printErr','wasmBinary','fkn']"

  # Memory: 64 MiB initial, grow as needed. Torrent buffers + Boost overhead
  # comfortably fit; pieces are paged through quickly.
  "-sINITIAL_MEMORY=67108864"
  "-sALLOW_MEMORY_GROWTH=1"
  "-sMAXIMUM_MEMORY=2147483648"

  # Helpers used by library_fkn.js — make sure they survive dead-code elim.
  "-sEXPORTED_FUNCTIONS=['_lt_session_create','_lt_session_destroy','_lt_session_tick','_lt_session_pump_alerts','_lt_alerts_size','_lt_alerts_data','_lt_alerts_clear','_lt_session_add_magnet','_lt_session_add_torrent_file','_lt_session_remove_torrent','_lt_torrent_status','_lt_torrent_post_status','_lt_torrent_infohash','_lt_disk_complete_read','_lt_disk_complete_write','_lt_disk_complete_simple','_lt_disk_complete_status','_lt_disk_complete_move','_lt_disk_complete_delete','_lt_disk_complete_rename','_lt_dns_complete','_lt_diag_tick_count','_lt_diag_total_handlers','_lt_diag_open_tcp','_lt_diag_open_udp','_lt_diag_listen_port','_lt_diag_force_reopen','_lt_diag_listen_full','_lt_diag_parse_interfaces','_malloc','_free']"
  "-sEXPORTED_RUNTIME_METHODS=['HEAPU8','HEAP32','HEAPU32','HEAP16','HEAPU16','UTF8ToString','stringToNewUTF8','lengthBytesUTF8']"

  # Our JS library: socket syscalls + disk bridge.
  "--js-library=${CMAKE_CURRENT_SOURCE_DIR}/js/library_fkn.js"
  "-sNO_DISABLE_EXCEPTION_CATCHING"
  "-fexceptions"
)

if (DEBUG_NAMES)
  list(APPEND EM_LINK_FLAGS "-sASSERTIONS=1" "--minify=0")
endif()

string(REPLACE ";" " " EM_LINK_FLAGS_STR "${EM_LINK_FLAGS}")
set_target_properties(libtorrent PROPERTIES
  OUTPUT_NAME "libtorrent"
  SUFFIX ".js"
  LINK_FLAGS "${EM_LINK_FLAGS_STR}"
)
