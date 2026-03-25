import requests


r=requests.get('https://opendata.dwd.de/climate_environment/CDC/observations_global/CLIMAT/monthly/raw/readme_RAW_CLIMATs_eng.txt')
content = r.text

i=0
while (True):
    if content.splitlines()[i].startswith('Gx'):
        break
    i+=1
print(i)

print(content.splitlines()[215])

var_defs=[i for i in content.splitlines() if '=' in i]
for each_def in var_defs:
    print(each_def)
    if each_def.startswith('nr'):
        print(f"Found {each_def}")
        temp=each_def.replace('millimetreS1', 'millimetre\nS1').splitlines()
        print(f"Found {temp}")
        each_def, later_def=tuple(temp)
        var_defs.append(later_def)
    var, defn = each_def.split('=')
    var, defn=var.strip(), defn.strip()
    print(f'{var}={defn}')
