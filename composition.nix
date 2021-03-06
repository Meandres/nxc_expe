{ pkgs, ... }: {
  nodes = {
    collector = { pkgs, ... }:
      {
        environment.systemPackages = [ pkgs.nur.repos.kapack.colmet-collector pkgs.nur.repos.kapack.colmet pkgs.killall ];
      };
    compute = { pkgs, ... }:
      { #We need the collector to have colmet-config-node
        environment.systemPackages = with pkgs; [ openmpi nur.repos.kapack.npb  nur.repos.kapack.colmet-collector nur.repos.kapack.colmet-rs nur.repos.kapack.colmet killall ];
        security.pam.loginLimits = [
            { domain = "*"; item = "memlock"; type = "-"; value = "unlimited"; }
            { domain = "*"; item = "stack"; type = "-"; value = "unlimited"; }
        ];
        environment.variables.OMPI_ALLOW_RUN_AS_ROOT = "1";
        environment.variables.OMPI_ALLOW_RUN_AS_ROOT_CONFIRM = "1";
      };
  };
  testScript = ''
    foo.succeed("true")
    '';
}
