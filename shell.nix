let
  sources = import ./nix/sources.nix;
  packages = import sources.nixpkgs {};
in
  packages.mkShell {
    buildInputs = [
      packages.jdk8_headless
      packages.maven
      packages.groovy
    ];
  }


