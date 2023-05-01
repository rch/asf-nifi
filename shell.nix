{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  packages = [
    pkgs.jdk17_headless
    pkgs.maven
  ];

  shellHook = ''
    export DEBUG=1
  '';
}

