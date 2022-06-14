{
  description = "Colmet benchmark experiment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-21.11";
    NUR.url = "github:nix-community/NUR";
    nxc.url = "git+https://gitlab.inria.fr/nixos-compose/nixos-compose.git";
    kapack.url = "/home/imeignanmasson/nur-kapack";
  };

  outputs = { self, nixpkgs, NUR, nxc, kapack }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackage.${system};
      nixos-compose = nxc.defaultPackage.${system};
      nxcEnv =  nixos-compose.dependencyEnv;

      bench_colmet = pkgs.writeScriptBin "bench_colmet" ''
        ${nxcEnv}/bin/python3 ${./nxc_benchmark_colmet_rs.py} $@
      '';

    in {

      packages.${system} = nxc.lib.compose {
        inherit nixpkgs system NUR;
        repoOverrides = { inherit kapack; };
        composition = ./composition.nix;
      };
      apps.${system} = {
        expe = {
          type = "app";
          program = "${bench_colmet}/bin/nxc_benchmark_colmet_rs";
        };
      };

      defaultPackage.${system} =
        self.packages.${system}."composition::bench_colmet";

      devShell.${system} = nxc.devShells.${system}.nxcShell;
    };
}
