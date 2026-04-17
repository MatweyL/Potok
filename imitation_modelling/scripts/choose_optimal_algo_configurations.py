import json
import pprint
from pathlib import Path


def main():
    base_dir = Path(__file__).parent.parent.joinpath('simulation_results_one_system')
    alogs = []
    for file in base_dir.iterdir():
        data = json.loads(file.read_text('utf-8'))
        last_metrics = data['history'][-1]
        completed = (last_metrics['completed'])
        total = (last_metrics['total'])
        if completed == total and 'v2' not in data['params']['task_batch_provider_params']['description']:
            alogs.append(data['params']['task_batch_provider_params']['description'])
    alogs.sort()
    pprint.pprint(alogs)
    print(len(alogs))


if __name__ == '__main__':
    main()
