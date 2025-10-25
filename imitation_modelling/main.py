from imitation_modelling.schemas import SimulationParams
from imitation_modelling.system_runner import build_system_runner


def main():
    params = SimulationParams()
    system_runner = build_system_runner(params)
    system_runner.run()


if __name__ == '__main__':
    main()
