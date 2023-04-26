import subprocess
import glob
import timeit
inicio = timeit.default_timer()



#UPDATE Data
# Define the commands to run in parallel
commands = ['python MAIN_3G.py','python MAIN_4G.py', 'python MAIN_5G.py']
#commands = ['python MAIN_3G.py']

# Run the commands in parallel
processes = []
for cmd in commands:
    processes.append(subprocess.Popen(cmd, shell=True))
# Wait for all processes to complete
for process in processes:
    process.wait()


   



#RUN in Parallel
#Processing data
py_files = glob.glob("*.py")
#Excluded
KeepListCompared = ['MAIN.py','MAIN_POOL.py','MAIN_3G.py','MAIN_4G.py', 'MAIN_5G.py','util.py']
locationBase_comparePMO = py_files
DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
commands = []
for i in DellListComparede:
    x = 'python '+i
    commands.append(x)
processes = []
for cmd in commands:
    processes.append(subprocess.Popen(cmd, shell=True))

# Wait for all processes to complete
for process in processes:
    process.wait()






#MERGE
# Define the commands to run in parallel
commands = [["python", "-c", "from DUMP import DUMP; DUMP('3G')"],
            ["python", "-c", "from DUMP import DUMP; DUMP('4G')"],
            ["python", "-c", "from DUMP import DUMP; DUMP('5G')"]]

#commands = [["python", "-c", "from DUMP import DUMP; DUMP('4G')"]]
# Run the commands in parallel
processes = []
for cmd in commands:
    processes.append(subprocess.Popen(cmd, shell=True))
# Wait for all processes to complete
for process in processes:
    process.wait()














fim = timeit.default_timer()
print ('duracao TOTAL[FINAL]: %.2f' % ((fim - inicio)/60) + ' min')