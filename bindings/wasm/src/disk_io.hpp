#pragma once

#include <memory>
#include "libtorrent/io_context.hpp"
#include "libtorrent/disk_interface.hpp"
#include "libtorrent/settings_pack.hpp"

namespace libtorrent {

struct counters;

// Factory installed into session_params::disk_io_constructor. Bridges every
// async disk op out to JS callbacks defined in js/library_fkn.js, then
// completes them asynchronously when JS calls back into the module.
std::unique_ptr<disk_interface> wasm_disk_io_constructor(
    io_context& ios, settings_interface const&, counters& cnt);

} // namespace libtorrent
