from typing import Type

from pydantic import BaseModel


def print_json_schema(model: Type[BaseModel]):
    print("\n".join([f"{k} - {v.get('description')}; Тип: {v.get('type')}" for k, v in
                     model.model_json_schema()['properties'].items()]))
