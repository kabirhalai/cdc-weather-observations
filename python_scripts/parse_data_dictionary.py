import requests
import yaml
import re
from collections import defaultdict


def parse_data_dictionary():
    r = requests.get(
        "https://opendata.dwd.de/climate_environment/CDC/observations_global/CLIMAT/monthly/raw/readme_RAW_CLIMATs_eng.txt"
    )
    content = r.text

    var_dict=defaultdict(dict)
    for section_num, text in enumerate(re.split(r'Parameters of section*', content)):
        section_lst = []
        for b in text.split('\r\n\r\n'):
            if '=' in b:
                _var, desc=b.split('=')[0],b.split('=')[1]
                section_lst.append({
                    "name": _var.strip(), 'description': f"{' '.join(desc.strip().splitlines())}"
                })
        var_dict[section_num] = section_lst

    return var_dict

def write_to_yaml(var_dict):
    with open("models/sources.yaml", mode="rt", encoding="utf-8") as file:
        dic=yaml.safe_load(file)
    for i in range(4):
        dic['sources'][0]['tables'][i]['columns']=var_dict[i+1]
    with open("models/sources.yaml", mode="wb") as _file:
        yaml.safe_dump(dic, _file,  encoding="utf-8", default_flow_style=False, sort_keys=False)
    return


if __name__ == "__main__":
    var_dict = parse_data_dictionary()
    write_to_yaml(var_dict)
