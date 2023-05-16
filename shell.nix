let
  sources = import ./nix/sources.nix;
  packages = import sources.nixpkgs {};
in
  packages.mkShell {
    buildInputs = [
      packages.jdk11_headless
    ];
  }


