from execo import *
from execo_g5k import *
from execo_engine import *
import yaml
import shutil

class experiment_plan_generator():
    def __init__(self, filename):
        shutil.rmtree("sweeps", ignore_errors=True)
        with open(filename, "r") as f:
            expe = yaml.safe_load(f)
        rep=expe['repetitions']
        expe['repetitions']=list()
        for i in range(0, rep):
            expe['repetitions'].append(i)
        self.sweeper=ParamSweeper("sweeps", sweep(expe))

    def get_next_config(self):
        config=self.sweeper.get_next()
        return ";".join(map(str, config.values()))

    def get_stats(self):
        return self.sweeper.stats()

    def get_percentage_remaining(self):
        return int(len(self.sweeper.get_remaining())/len(self.sweeper.get_sweeps())*100)
    
    def get_nb_remaining(self):
        return len(self.sweeper.get_remaining())

    def get_nb_total(self):
        return len(self.sweeper.get_sweeps())

if __name__ == "__main__":
    logger.setLevel(0)
    plan=experiment_plan_generator("expe.yml")
    print(plan.get_nb_remaining())
    print(plan.get_next_config())
