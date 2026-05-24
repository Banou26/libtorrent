{
  description = "libtorrent → WASM dev shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      forSystems = f: nixpkgs.lib.genAttrs
        [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ]
        (system: f (import nixpkgs { inherit system; }));
    in
    {
      devShells = forSystems (pkgs: {
        default = pkgs.mkShell {
          name = "libtorrent-wasm";
          packages = with pkgs; [
            emscripten     # emcc, em++, emcmake
            cmake
            ninja
            pkg-config
            boost          # header-only paths we need (asio, system, intrusive)
            nodejs_22      # runs vite + the example harness
            git
          ];

          # Emscripten needs a writable cache. Default ~/.cache/emscripten works,
          # but for reproducibility we point it at a project-local dir.
          shellHook = ''
            export EM_CACHE="$PWD/.em_cache"
            export BOOST_ROOT="${pkgs.boost.dev}/include"
            mkdir -p "$EM_CACHE"
            echo "emcc: $(emcc --version | head -1)"
            echo "BOOST_ROOT=$BOOST_ROOT"
            echo
            echo "Build:  emcmake cmake -B build -S . -DCMAKE_BUILD_TYPE=Release && cmake --build build -j"
            echo "Test:   (cd example && npm install && npm run dev)"
          '';
        };
      });
    };
}
