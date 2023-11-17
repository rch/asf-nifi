let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {};
in
with pkgs;
pkgs.mkShell {
  buildInputs = [
    bash
    git
    jdk8_headless
    maven
    python311
  ];
}

