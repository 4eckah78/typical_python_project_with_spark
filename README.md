# typical_python_project
This repository provides an example of a typical Python project with spark

**NOTE** 

I downloaded all folders (including "junk" ones), so you can see, what i've used during the developing

**FUNCTIONALITY**

This app takes a csv file and a year and returns the city and the month with highest temperature in the given year

**USAGE**
- run 
```bash
python -m pip install dist\weather-0.0.1-py3-none-any.whl
```
- then run
```bash
python -m weather.main {csv file} {year}
```

**EXAMPLE**
- input
```bash
python -m weather.main GlobalLandTemperaturesByMajorCity.csv 1985
```
- output
```bash
The hottest temperature in 1985 was in Riyadh in 8 month
```