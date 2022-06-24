from execo import *
from execo_g5k import *
from execo_engine import Engine
from experiment_plan import experiment_plan_generator
from nixos_compose.nxc_execo import get_oar_job_nodes_nxc, build_nxc_execo
import traceback
import logging
import time
import os
import sys
import threading
username=""
starttime=0

def parse_output(s):
    lines=s.splitlines()
    out=""
    for l in lines:
        if l.startswith(" Time in seconds"):
            sp=l.split(" ")
            out+=sp[-1]
        elif l.startswith(" Mop/s total"):
            sp=l.split(" ")
            out+=";"+sp[-1]
    return out
    
def reserve_nodes(nb_nodes, site, cluster, walltime=3600):
    jobs=oarsub([(OarSubmission(resources="cluster=1/nodes={}".format(nb_nodes), walltime=walltime, additional_options="-O /dev/null -E /dev/null"), site)])
    return jobs

def write_nodefile(nodes):
    hf=open("{}/nodefile".format(os.getcwd()),"w")
    for n in nodes:
        print(n)
        for _ in range(0, int(get_host_attributes(n)['architecture']['nb_cores'])):
            hf.write(n+"\n")
    hf.close()
    for n in nodes:
        _ = Process("scp {}/nodefile root@{}:/root".format(os.getcwd(), n)).run()

class Colmet_bench(Engine):
    """
    
    """
    def __init__(self):
        super(Colmet_bench, self).__init__()
        parser = self.args_parser
        parser.add_argument('--nxc_build_file', help='Path to the NXC deploy file')
        parser.add_argument('--build', action='store_true', help='Build the composition')
        parser.add_argument('--nxc_folder', default="~/nixProjects/nixos-compose", help="Path to the NXC folder")
        parser.add_argument('--experiment_file', help="File describing the experiment to perform", default="expe_parameters.yml")
        parser.add_argument('--result_file', help="Beginning of the name of the output file. The rest will be the benchmark launched and the .csv extension.", default="expe_results")
        parser.add_argument('--time_experiment', default=300, help="Time needed to perform one repetition (in sec)")
        parser.add_argument('--site', default="grenoble", help="G5K site where the submission will be issued")
        parser.add_argument('--cluster', default="dahu", help="G5K cluster from where nodes should be requested")
        parser.add_argument('-v', '--verbose', action='count', dest="verbosity", default=1)
        parser.add_argument('-n', '--nb_cmp_nodes', dest="number_compute_nodes", default=8)
        parser.add_argument('--name_bench', default="ep")
        parser.add_argument('--class_bench', default="E")
        parser.add_argument('--type_bench', default="mpi")
        self.nodes = {}
        self.oar_job_id = -1
        self.colmet_launched = False

    def init(self):
        logger.setLevel(40 - self.args.verbosity * 10)
        self.plan = experiment_plan_generator(self.args.experiment_file)
        nxc_build_file = None
        if self.args.build:
            (nxc_build_file, _time, _size) = build_nxc_execo(self.args.nxc_folder, self.args.site, self.args.cluster, walltime=15*60)
        elif self.args.nxc_build_file is not None:
            nxc_build_file = self.args.nxc_build_file
        else:
            raise Exception("No compose info file")
        oar_job = reserve_nodes(int(self.args.number_compute_nodes)+1, self.args.site, self.args.cluster, walltime = self.plan.get_nb_remaining()*self.args.time_experiment)
        self.oar_job_id, site = oar_job[0]
        roles = {"collector":1, "compute":self.args.number_compute_nodes}
        node_hostnames = get_oar_job_nodes(self.oar_job_id, self.args.site)
        self.nodes = get_oar_job_nodes_nxc(self.oar_job_id, self.args.site, compose_info_file=nxc_build_file, roles_quantities=roles)
        compute_hosts = [ node_hostnames[i].address for i in range(1, len(node_hostnames)) ]
        self.plan = experiment_plan_generator(self.args.experiment_file)
        write_nodefile(compute_hosts)

    def kill_colmet(self, type_colmet):
        """Killing colmet node agent on all the compute nodes"""
         # We assign to nothing to suppress outputs
        _ = self.colmet_nodes.kill()
        _ = self.collector.kill()
        if (type_colmet == "Python"):
            _ = SshProcess("killall .python-collect", self.nodes["collector"][0], connection_params={'user':'root'}).run()
        _ = self.colmet_nodes.wait()
        _ = self.collector.wait()
        self.colmet_launched = False

    def update_colmet(self, parameters):
        if self.colmet_launched :
            self.kill_colmet(parameters['type_colmet'])
        if (parameters['type_colmet']=="Rust" or parameters['type_colmet']=="Python"):
            if (parameters['type_colmet']=="Rust"):
                node_command = "colmet-node --enable-perfhw -s {} -m {} --zeromq-uri tcp://{}:5556".format(parameters["sampling_period"], parameters["metrics"], self.nodes["collector"][0].address)
                collector_command = "colmet-collector"
            else:
                node_command = "python-node -s {} --zeromq-uri tcp://{}:5556".format(parameters["sampling_period"], self.nodes["collector"][0].address)
                collector_command = "python-collector -s {} --enable-stdout-backend".format(parameters["sampling_period"])
            self.colmet_nodes = Remote(node_command, self.nodes["compute"], connection_params={"user" : "root"}).start()
            self.collector = SshProcess(collector_command, self.nodes["collector"][0], connection_params={'user' : 'root'}).start()
            self.colmet_launched = True

    def run(self):
        self.uniform_parameters = {
                'bench_name':self.args.name_bench,
                'bench_class':self.args.class_bench,
                'bench_type':self.args.type_bench,
                'nb_nodes':self.args.number_compute_nodes
                }
        a = input("Stop")
        f = open(self.args.result_file + "_" + self.uniform_parameters['bench_name'] + "_" + self.uniform_parameters['bench_class'] + "_" + self.uniform_parameters['bench_type']+".csv", "w")
        f.write(str(self.uniform_parameters))
        f.write("repetition,sampling,metrics,time,Mops\n")
        for i in range(self.plan.get_nb_total()):
            print("Remaining : {}%".format(self.plan.get_percentage_remaining()))
            out = self.do_repetition(self.plan.get_next_config())
            f.write(out)
        f.close()


    def do_repetition(self, parameters):
        """Execute the bench for a given combination of parameters."""
        mpi_executable_name = self.uniform_parameters['bench_name'] + "." + self.uniform_parameters['bench_class'] + "." + self.uniform_parameters['bench_type']

        self.update_colmet(parameters)
        
        bench_command = "mpirun --mca pml ^ucx --mca mtl ^psm2,ofi --mca btl ^ofi,openib -machinefile {}/nodefile ".format(os.getcwd()) + mpi_executable_name
    
        p = SshProcess(bench_command, self.nodes['compute'][0], connection_params={"user":"root"}).run(timeout=self.args.time_experiment)
        p.wait()
        return "{repetition},{type_colmet},{sampling_period},{metrics}".format(**parameters)+parse_output(p.stdout)+"\n"

if __name__ == "__main__":
    bench = Colmet_bench()
    try:
        bench.start()
        oardel([(bench.oar_job_id, None)])
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
        oardel([(bench.oar_job_id, None)])
